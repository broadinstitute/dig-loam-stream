package loamstream.loam.intake

import scala.util.matching.Regex
import loamstream.util.Files
import java.io.Reader
import java.io.FileReader
import java.nio.file.Paths
import java.nio.file.Files.exists
import loamstream.util.CanBeClosed
import java.io.File
import loamstream.util.Options
import java.nio.file.Path
import org.apache.commons.csv.CSVParser
import loamstream.util.TimeUtils
import loamstream.util.Loggable

object FlipDetector extends Loggable {
  def default: FlipDetector = new FlipDetector
  
  private def currentDir: Path = Paths.get(".")
  
  private val n2c: String => String = {
    case "A" => "T"
    case "C" => "G"
    case "T" => "A"
    case "G" => "C"
  }
  
  private final class FileHandle(file: java.io.File) {
    private def newReader = new FileReader(file)
    
    def readAt(i: Long): Option[Char] = {
      CanBeClosed.enclosed(newReader) { reader =>
        val numSkipped = reader.skip(i)
      
        if(numSkipped == i) { Some(reader.read().toChar) } 
        else { None }
      }
    }
    
    def readAt(start: Long, length: Int): Option[String] = {
      val arr: Array[Char] = Array.ofDim(length)
      
      CanBeClosed.enclosed(newReader) { reader =>
        def read(): Option[String] = {
          val numRead = reader.read(arr, 0, length)
        
          if(numRead == length) Some(arr.mkString) else None
        }
        
        val numSkipped = reader.skip(start)
        
        if(numSkipped == start) read() else None
      }
    }
  }
  
  private final class ReferenceFiles private (chromsToFiles: Map[String, FileHandle]) {
    def isKnown(chrom: String): Boolean = chromsToFiles.contains(chrom)
    
    def getChar(chrom: String, position: Long): Option[Char] = chromsToFiles.get(chrom).flatMap(_.readAt(position))
    
    def getString(chrom: String, position: Long, length: Int): Option[String] = {
      chromsToFiles.get(chrom).flatMap(_.readAt(position, length))
    }
    
    def forChrom(chrom: String): FileHandle = chromsToFiles(chrom)
  }
  
  private object ReferenceFiles {
    def apply(referenceDir: Path, knownChroms: Set[String]): ReferenceFiles = TimeUtils.time("Making ReferenceFiles", info(_)) {
      new ReferenceFiles(Map.empty ++ {
        for {
          chrom <- knownChroms.iterator
        } yield {
          val path = referenceDir.resolve(s"${chrom}.txt")
          
          require(exists(path), s"ERROR: no sequence file for chromosome: ${chrom}")
          
          chrom -> new FileHandle(path.toFile)
        }
      })
    }
  }
  
  private final class Variant(
      referenceFiles: ReferenceFiles,
      variantsFrom26k: Set[String],
      val chrom: String, 
      val position: Int, 
      val alt: String, 
      val reference: String) {
    def toKey: String = s"${chrom}_${position}_${alt}_${reference}"
    
    def toKeyMunged: String = s"${chrom}_${position}_${n2c(alt)}_${n2c(reference)}"
    
    def isIn26k: Boolean = variantsFrom26k.contains(this.toKey)
    
    def isIn26kMunged: Boolean = variantsFrom26k.contains(this.toKeyMunged)
    
    def refChar: Option[Char] = referenceFiles.getChar(chrom, position - 1) 
    
    def refFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, reference.size)
    
    def altFromReferenceGenome: Option[String] = referenceFiles.getString(chrom, position - 1, alt.size)
  }
  
  private object Variant {
    final case class Extractor(regex: Regex, referenceFiles: ReferenceFiles, variantsFrom26k: Set[String]) {
      def unapply(s: String): Option[Variant] = s match {
        case regex(c, p, a, r) => Some(new Variant(referenceFiles, variantsFrom26k, c, p.toInt, a, r))
        case _ => None
      }
    }
  }
}

final class FlipDetector(
    referenceDir: Path = FlipDetector.currentDir.resolve("reference"),
    isVarDataType: Boolean = false,
    pathTo26kMap: Path = Paths.get("/humgen/diabetes2/users/mvg/portal/scripts/26k_id.map")) extends Loggable {
  
  private def chromsFromReference: Iterator[String] = TimeUtils.time("Listing chrom files", info(_)) {
    import scala.collection.JavaConverters._
        
    val referenceFiles = java.nio.file.Files.list(referenceDir).iterator.asScala
    
    val txtFiles = referenceFiles.filter(_.getFileName.endsWith("txt"))
    
    val fileNameNoExtensionRegex = """(.+)\.txt""".r
    
    txtFiles.map(_.toString).collect { case fileNameNoExtensionRegex(fileName) => fileName }
  }
  
  private val knownChroms: Set[String] = {
    def chromsIfVarDataType = if(isVarDataType) chromsFromReference else Iterator.empty
    
    Set("X") ++ (1 to 22).map(_.toString) ++ chromsIfVarDataType
  }

  private val variantsFrom26k: Set[String] = TimeUtils.time("Reading 26k map", info(_)) {
    /*CanBeClosed.enclosed(new FileReader(pathTo26kMap.toFile)) { fileReader =>
      CanBeClosed.enclosed(new CSVParser(fileReader, CsvSource.Defaults.tabDelimited)) { csvParser =>
        import scala.collection.JavaConverters._
        
        val iterator = csvParser.iterator.asScala
        
        iterator.map(_.get(0)).toSet
      }
    }*/
    
    import scala.collection.JavaConverters._
    
    java.nio.file.Files.lines(pathTo26kMap).iterator.asScala.map(line => line.split("\t").apply(0)).toSet
  }

  private lazy val referenceFiles = FlipDetector.ReferenceFiles(referenceDir, knownChroms)
  
  def isFlipped(variantId: String): Boolean = TimeUtils.time("Testing for flipped-ness", info(_)) {
    def isKnown(chrom: String): Boolean = knownChroms.contains(chrom)
    
    def isValidKey: Boolean = variantsFrom26k.contains(variantId) && !variantId.contains(",")
    
    isValidKey && {
      val regex1 = """^(.+)_([0-9]+)_([ATGC])_([ATGC])$""".r
      val regex2 = """^(.+)_([0-9]+)_([ATGC]+)_([ATGC]+)$""".r
  
      val extractor1 = FlipDetector.Variant.Extractor(regex1, referenceFiles, variantsFrom26k)
      val extractor2 = FlipDetector.Variant.Extractor(regex2, referenceFiles, variantsFrom26k)
      
      import Options.Implicits._
      import FlipDetector.n2c
      
      variantId match {
        case extractor1(variant) => {
          import variant.{reference, alt}

          variant.isIn26k ||
          variant.isIn26kMunged ||
          variant.refChar.map { refChar => 
            val ref = refChar.toString
        
            (ref != reference) && { (ref == alt) || (ref == n2c(alt)) }
          }.orElseFalse
        }// case regex1
        case extractor2(variant) => {
          import variant.{reference, alt}
          
          def munge(s: String): String = {
            s.replaceAll("A", "X").replaceAll("T", "A").replaceAll("X", "T").replaceAll("C", "X").replaceAll("G", "C").replaceAll("X", "G")
          }
          
          (for {
            ref <- variant.refFromReferenceGenome
            if ref != reference
            altFromReferenceGenome <- variant.altFromReferenceGenome
          } yield {
            (altFromReferenceGenome == alt) || (altFromReferenceGenome == munge(alt))
          }).orElseFalse
        } //case regex2
        case _ => false
      }
    }
  }
}

package loamstream.loam.intake

import java.io.FileReader
import java.nio.file.Files.exists
import java.nio.file.Path
import java.nio.file.Paths

import scala.util.matching.Regex

import loamstream.util.CanBeClosed
import loamstream.util.Loggable
import loamstream.util.TimeUtils

/**
 * Tests variants for allele-flipping. 
 * 
 * A failry direct port of one of Marcin's Perl scripts. 
 */
final class FlipDetector(
    referenceDir: Path = FlipDetector.currentDir.resolve("reference"),
    isVarDataType: Boolean = false,
    pathTo26kMap: Path = Paths.get("/humgen/diabetes2/users/mvg/portal/scripts/26k_id.map")) extends Loggable {
  
  def isFlipped(variantId: String): Boolean = TimeUtils.time("Testing for flipped-ness", trace(_)) {
    def isValidKey: Boolean = variantsFrom26k.contains(variantId) && !variantId.contains(",")
    
    import Regexes.{ regex1, regex2 }
    
    isValidKey && {
      val extractor1 = extractor(regex1)
      val extractor2 = extractor(regex2)
      
      variantId match {
        case extractor1(variant) => method1(variant)
        case extractor2(variant) => method2(variant)
        case _ => false
      }
    }
  }
  
  private def chromsFromReference: Iterator[String] = TimeUtils.time("Listing chrom files", debug(_)) {
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

  private val variantsFrom26k: Set[String] = TimeUtils.time("Reading 26k map", debug(_)) {
    val iterator = CsvSource.FastCsv.fromFile(pathTo26kMap, containsHeader = false).records
    
    iterator.map(_.getFieldByIndex(0)).toSet
  }

  private lazy val referenceFiles = FlipDetector.ReferenceFiles(referenceDir, knownChroms)
  
  
  private def method1(variant: FlipDetector.Variant): Boolean = {
    import variant.{ alt, reference }
    import FlipDetector.n2c
    import loamstream.util.Options.Implicits._
    
    variant.isIn26k ||
    variant.isIn26kMunged ||
    variant.refChar.map { refChar => 
      val ref = refChar.toString
  
      (ref != reference) && { (ref == alt) || (ref == n2c(alt)) }
    }.orElseFalse
  }
  
  private def method2(variant: FlipDetector.Variant): Boolean = {
    def munge(s: String): String = {
        s.replaceAll("A", "X")
         .replaceAll("T", "A")
         .replaceAll("X", "T")
         .replaceAll("C", "X")
         .replaceAll("G", "C")
         .replaceAll("X", "G")
      }
      
    import variant.{ alt, reference }
    import loamstream.util.Options.Implicits._
    
    (for {
      ref <- variant.refFromReferenceGenome
      if ref != reference
      altFromReferenceGenome <- variant.altFromReferenceGenome
    } yield {
      (altFromReferenceGenome == alt) || (altFromReferenceGenome == munge(alt))
    }).orElseFalse
  }
  
  private def extractor(regex: Regex): FlipDetector.Variant.Extractor = {
    FlipDetector.Variant.Extractor(regex, referenceFiles, variantsFrom26k)
  }
  
  object Regexes {
    val regex1: Regex = """^(.+)_([0-9]+)_([ATGC])_([ATGC])$""".r
    val regex2: Regex = """^(.+)_([0-9]+)_([ATGC]+)_([ATGC]+)$""".r
  }
}

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
    def apply(referenceDir: Path, knownChroms: Set[String]): ReferenceFiles = {
      TimeUtils.time("Making ReferenceFiles", debug(_)) {
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

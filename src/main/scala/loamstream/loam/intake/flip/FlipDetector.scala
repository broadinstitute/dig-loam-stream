package loamstream.loam.intake.flip

import java.nio.file.Files.exists
import java.nio.file.Path
import java.nio.file.Paths
import scala.util.matching.Regex
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.loam.intake.CsvSource

/**
 * Tests variants for allele-flipping. 
 * 
 * A fairly direct port of one of Marcin's Perl scripts. 
 */
trait FlipDetector {
  def isFlipped(variantId: String): Boolean
}

object FlipDetector extends Loggable {
  
  object Defaults {
    val referenceDir: Path = currentDir.resolve("reference")
    val isVarDataType: Boolean = false
    val pathTo26kMap: Path = Paths.get("/humgen/diabetes2/users/mvg/portal/scripts/26k_id.map")
  }

  def default: FlipDetector = new FlipDetector.Default()
  
  private def currentDir: Path = Paths.get(".")
  
  final class Default(
      referenceDir: Path = Defaults.referenceDir,
      isVarDataType: Boolean = Defaults.isVarDataType,
      pathTo26kMap: Path = Defaults.pathTo26kMap) extends FlipDetector with Loggable {
    
    override def isFlipped(variantId: String): Boolean = TimeUtils.time("Testing for flipped-ness", trace(_)) {
      val isValidKey: Boolean = !variantsFrom26k.contains(variantId) && !variantId.contains(",")
      
      import Regexes.{ singleNucleotide, multiNucleotide }
      
      isValidKey && {
        val singleNucleotideExtractor = extractor(singleNucleotide)
        val multiNucleotideExtractor = extractor(multiNucleotide)
        
        variantId match {
          case singleNucleotideExtractor(variant) => handleSingleNucleotideVariant(variant)
          case multiNucleotideExtractor(variant) => handleMultiNucleotideVariant(variant)
          case _ => false
        }
      }
    }
    
    private def chromsFromReference: Iterator[String] = TimeUtils.time("Listing chrom files", debug(_)) {
      import scala.collection.JavaConverters._
          
      val referenceFiles = java.nio.file.Files.list(referenceDir).iterator.asScala
      
      def isTxtOrGzFile(file: Path): Boolean = {
        val fileName = file.getFileName.toString
        
        fileName.endsWith("txt") || fileName.endsWith("gz")
      }
      
      val txtFiles = referenceFiles.filter(isTxtOrGzFile)
      
      val fileNameNoExtensionRegex = """(.+?)\.(txt|gz)$""".r
      
      txtFiles.map(_.getFileName.toString).collect { 
        case fileNameNoExtensionRegex(fileName, _) =>  fileName
      }
    }
    
    private[flip] val knownChroms: Set[String] = {
      def chromsIfVarDataType = if(isVarDataType) chromsFromReference else Iterator.empty
      
      Set("X") ++ (1 to 22).map(_.toString) ++ chromsIfVarDataType
    }
  
    private val variantsFrom26k: Set[String] = TimeUtils.time("Reading 26k map", debug(_)) {
      val iterator = CsvSource.fromFile(pathTo26kMap, CsvSource.Defaults.Formats.tabDelimited).records
      
      iterator.map(_.getFieldByIndex(1)).toSet
    }
  
    private lazy val referenceFiles = ReferenceFiles(referenceDir, knownChroms)
    
    private def handleSingleNucleotideVariant(variant: RichVariant): Boolean = {
      import variant.{ alt, reference }
      import loamstream.util.Options.Implicits._
      
      variant.flip.isIn26k ||
      variant.flip.isIn26kMunged ||
      variant.refChar.map { refChar => 
        val ref = refChar.toString
    
        (ref != reference) && { (ref == alt) || (ref == N2C(alt)) }
      }.orElseFalse
    }
    
    private def handleMultiNucleotideVariant(variant: RichVariant): Boolean = {
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
    
    private def extractor(regex: Regex): RichVariant.Extractor = {
      RichVariant.Extractor(regex, referenceFiles, variantsFrom26k)
    }
  }
  
  private object Regexes {
    val singleNucleotide: Regex = """^(.+)_([0-9]+)_([ATGC])_([ATGC])$""".r
    val multiNucleotide: Regex = """^(.+)_([0-9]+)_([ATGC]+)_([ATGC]+)$""".r
  }
}

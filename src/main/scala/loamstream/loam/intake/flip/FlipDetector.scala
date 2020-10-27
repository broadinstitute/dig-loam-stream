package loamstream.loam.intake.flip

import java.nio.file.Files.exists
import java.nio.file.Path
import java.nio.file.Paths
import scala.util.matching.Regex
import loamstream.util.Loggable
import loamstream.util.TimeUtils
import loamstream.loam.intake.Source

/**
 * Tests variants for allele-flipping. 
 * 
 * A fairly direct port of one of Marcin's Perl scripts. 
 */
trait FlipDetector {
  def isFlipped(variantId: String): Disposition
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
    
    import Regexes.{ singleNucleotide, multiNucleotide }
    
    private val singleNucleotideExtractor = extractor(singleNucleotide)
    private val multiNucleotideExtractor = extractor(multiNucleotide)
    
    override def isFlipped(variantId: String): Disposition = {
      val isValidVariantId: Boolean = !variantsFrom26k.contains(variantId) && !variantId.contains(",")
      
      import Disposition.NotFlippedSameStrand
      
      if(isValidVariantId) {
        variantId match {
          case singleNucleotideExtractor(variant) => handleSingleNucleotideVariant(variant)
          case multiNucleotideExtractor(variant) => handleMultiNucleotideVariant(variant)
          case _ => NotFlippedSameStrand
        }
      } else {
        NotFlippedSameStrand  
      }
    }
    
    private def chromsFromReference: Iterator[String] = {
      import scala.collection.JavaConverters._
          
      val referenceFiles = java.nio.file.Files.list(referenceDir).iterator.asScala
      
      def isTxtOrGzFile(file: Path): Boolean = {
        val fileName = file.getFileName.toString
        
        fileName.endsWith("txt")
      }
      
      val txtFiles = referenceFiles.filter(isTxtOrGzFile)
      
      val fileNameNoExtensionRegex = """(.+?)\.(txt|gz)$""".r
      
      txtFiles.map(_.getFileName.toString).collect { 
        case fileNameNoExtensionRegex(fileName, _) =>  fileName
      }
    }
    
    private[flip] lazy val knownChroms: Set[String] = {
      def chromsIfVarDataType = if(isVarDataType) chromsFromReference else Iterator.empty
      
      Set("X") ++ (1 to 22).map(_.toString) ++ chromsIfVarDataType
    }
  
    private lazy val variantsFrom26k: java.util.Set[String] = TimeUtils.time("Reading 26k map", trace(_)) {
      val iterator = Source.fromFile(pathTo26kMap, Source.Formats.tabDelimited).records
      
      val result: java.util.Set[String] = new java.util.HashSet
      
      iterator.map(_.getFieldByIndex(1)).foreach(result.add)
      
      result
    }
  
    private lazy val referenceFiles = ReferenceFiles(referenceDir, knownChroms)
    
    private def handleSingleNucleotideVariant(variant: RichVariant): Disposition = {
      import variant.{ alt, reference }
      import Disposition._
      
      //TODO: IS THIS RIGHT??
      if(variant.flip.isIn26k) { println("IN 26k") ; FlippedSameStrand }
      else if(variant.isIn26kComplemented) { println(println("COMPLEMENT IN 26k")) ; NotFlippedComplementStrand }
      else if(variant.flip.isIn26kComplemented) { println("FLIPPED-COMPLEMENT IN 26k") ; FlippedComplementStrand }
      else {
        val refFromRefGenomeOpt = variant.refCharFromReferenceGenome.map(_.toString)
        
        implicit val v = variant
        
        refFromRefGenomeOpt match {
          case RefMatchesAltInstead() => FlippedSameStrand
          case RefMatchesAltComplementInstead() => FlippedComplementStrand
          case RefMatchesRefComplement() => NotFlippedComplementStrand
          case _ => NotFlippedSameStrand
        }
      }
    }
    
    private def handleMultiNucleotideVariant(variant: RichVariant): Disposition = {
      import variant.{ alt, reference }
      import loamstream.util.Options.Implicits._
      import Disposition._
      
      variant.refFromReferenceGenome.filter(_ != reference).zip(variant.altFromReferenceGenome) match {
        case Some((_, altFromRefGenome)) if altFromRefGenome == alt => FlippedSameStrand
        case Some((_, altFromRefGenome)) if altFromRefGenome == Complement(alt) => FlippedComplementStrand
        case _ => NotFlippedSameStrand
      }
    }
    
    private def extractor(regex: Regex): RichVariant.Extractor = {
      RichVariant.Extractor(regex, referenceFiles, variantsFrom26k)
    }
  }
  
  private object RefMatchesAltInstead {
    def unapply(refFromRefGenomeOpt: Option[String])(implicit v: RichVariant): Boolean = refFromRefGenomeOpt match {
      case Some(refFromRefGenome) => refFromRefGenome != v.reference && refFromRefGenome == v.alt
      case _ => false
    }
  }
  
  private object RefMatchesAltComplementInstead {
    def unapply(refFromRefGenomeOpt: Option[String])(implicit v: RichVariant): Boolean = refFromRefGenomeOpt match {
      case Some(refFromRefGenome) => refFromRefGenome != v.reference && refFromRefGenome == Complement(v.alt)
      case _ => false
    }
  }
  
  private object RefMatchesRefComplement {
    def unapply(refFromRefGenomeOpt: Option[String])(implicit v: RichVariant): Boolean = refFromRefGenomeOpt match {
      case Some(refFromRefGenome) => refFromRefGenome == Complement(v.reference)
      case _ => false
    }
  }
  
  private object Regexes {
    val singleNucleotide: Regex = """^(.+)_([0-9]+)_([ATGC])_([ATGC])$""".r
    val multiNucleotide: Regex = """^(.+)_([0-9]+)_([ATGC]+)_([ATGC]+)$""".r
  }
}

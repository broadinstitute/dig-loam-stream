package loamstream.loam.intake

import java.nio.file.Paths

import loamstream.util.Loggable
import loamstream.util.TimeUtils

/**
 * @author clint
 * Feb 25, 2020
 */
object DslExample1 extends App with Loggable {
  
  import IntakeSyntax._
  
  object ColumnDefs {
    import ColumnNames._
    
    val varId = ColumnDef(
      VARID,
      strexpr"${CHROM}_${POS}_${Allele2.asUpperCase}_${Allele1.asUpperCase}",
      strexpr"${CHROM}_${POS}_${Allele1.asUpperCase}_${Allele2.asUpperCase}")
    
    val chrom = ColumnDef(CHROM)
    val pos = ColumnDef(POS)
    
    val referenceAllele = ColumnDef(ReferenceAllele, Allele2.asUpperCase, Allele1.asUpperCase)
      
    val effectAllele = ColumnDef(EffectAllele, Allele1.asUpperCase, Allele2.asUpperCase)
    
    val effectAllelePh = ColumnDef(EffectAllelePH, Allele1.asUpperCase, Allele2.asUpperCase)
      
    val eaf = ColumnDef(EAF, Freq1, Freq1.asDouble.complement)
      
    val eafPh = ColumnDef(EAFPH, Freq1, Freq1.asDouble.complement)
      
    val maf = ColumnDef(MAF, Freq1.asDouble.complementIf(_ > 0.5))
      
    val mafPh = ColumnDef(MAFPH, Freq1.asDouble.complementIf(_ > 0.5))
      
    val oddsRatio = ColumnDef(OddsRatio, Effect.asDouble.exp, Effect.asDouble.negate.exp)
      
    val se = ColumnDef(SE, StdErr, StdErr)

    val pv = ColumnDef(PValue, PDashValue, PDashValue)
  }
  
  object ColumnNames {
    val VARID = "VAR_ID".asColumnName
    val CHROM = "CHROM".asColumnName
    val POS = "POS".asColumnName
    val Allele1 = "Allele1".asColumnName
    val Allele2 = "Allele2".asColumnName
    val ReferenceAllele = "Reference_Allele".asColumnName
    val EffectAllele = "Effect_Allele".asColumnName
    val EffectAllelePH = "Effect_Allele_PH".asColumnName
    val EAF = "EAF".asColumnName
    val EAFPH = "EAF_PH".asColumnName
    val MAF = "MAF".asColumnName
    val MAFPH = "MAF_PH".asColumnName
    val OddsRatio = "ODDS_RATIO".asColumnName
    val SE = "SE".asColumnName
    val PValue = "P_VALUE".asColumnName
    val Freq1 = "Freq1".asColumnName
    val Effect = "Effect".asColumnName
    val StdErr = "StdErr".asColumnName
    val PDashValue = "P-value".asColumnName
  }
  
  val src: CsvSource = CsvSource.fromCommandLine("cat data.txt")
  
  val (header, rows) = TimeUtils.time("Making DataRow iterator", info(_)) {
    val flipDetector = new FlipDetector(
      referenceDir = Paths.get("/home/clint/workspace/marcins-scripts/reference"),
      isVarDataType = true,
      pathTo26kMap = Paths.get("/home/clint/workspace/marcins-scripts/26k_id.map"))
    
    val rowDef = RowDef(
      ColumnDefs.varId.from(src), 
      src.producing(Seq(
        ColumnDefs.chrom, 
        ColumnDefs.pos, 
        ColumnDefs.referenceAllele,
        ColumnDefs.effectAllele,
        ColumnDefs.effectAllelePh,
        ColumnDefs.eaf,
        ColumnDefs.eafPh,
        ColumnDefs.maf,
        ColumnDefs.mafPh,
        ColumnDefs.oddsRatio,
        ColumnDefs.se,
        ColumnDefs.pv)))
    
    process(flipDetector)(rowDef)
  }
  
  val renderer = CommonsCsvRenderer(CsvSource.Defaults.Formats.tabDelimitedWithHeaderCsvFormat)

  val s = TimeUtils.time("processing", info(_)) {
    rows.size
  }
}

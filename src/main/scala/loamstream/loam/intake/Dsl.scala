package loamstream.loam.intake

import java.nio.file.Path
import java.nio.file.Paths
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVRecord
import loamstream.util.TakesEndingActionIterator
import loamstream.util.BashScript

import scala.language.implicitConversions
import scala.language.dynamics
import loamstream.util.Sequence
import loamstream.util.Maps
import java.io.FileReader
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import loamstream.util.ExecutorServices
import scala.concurrent.Await
import loamstream.util.TimeUtils
import loamstream.util.Loggable

object Dsl extends App with Loggable {
  
  //val src: CsvSource = CsvSource.FromFile(Paths.get("data.txt"))
  val src: CsvSource = CsvSource.FromCommand("cat data.txt")

  object ColumnNames {
    import ColumnName.ColumnNameOps
    
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
    
    /*VAR_ID	${}
  CHROM   $CHROM	$CHROM	"zcat some/file | add_chrom_pos"
  
  Effect_Allele   =uc(Allele1)    =uc(Allele2)    "zcat some/file | add_chrom_pos"
  Effect_Allele_PH        =uc(Allele1)    =uc(Allele2)    "zcat some/file | add_chrom_pos"
  EAF     Freq1   =1-Freq1        "zcat some/file | add_chrom_pos"
  EAF_PH  Freq1   =1-Freq1        "zcat some/file | add_chrom_pos"
  MAF     =Freq1 > 0.5 ? 1-Freq1 : Freq1  DONOTHING       "zcat some/file | add_chrom_pos"
  MAF_PH  =Freq1 > 0.5 ? 1-Freq1 : Freq1  DONOTHING       "zcat some/file | add_chrom_pos"
  ODDS_RATIO      =exp(Effect)    =exp(-Effect)   "zcat some/file | add_chrom_pos"
  SE      StdErr  StdErr  "zcat some/file | add_chrom_pos"
  P_VALUE P-value P-value "zcat some/file | add_chrom_pos"
  * 
  */
  }
  
  object ColumnDefs {
    import Interpolators._
    import ColumnNames._
    
    val varId = ColumnDefinition(
      VARID,
      strexpr"${CHROM}_${POS}_${Allele2 ~> (_.toUpperCase)}_${Allele1 ~> (_.toUpperCase)}",
      strexpr"${CHROM}_${POS}_${Allele1 ~> (_.toUpperCase)}_${Allele2 ~> (_.toUpperCase)}",
      src)
    
    val chrom: ColumnDefinition = ColumnDefinition(CHROM, src)
    val pos: ColumnDefinition = ColumnDefinition(POS, src)
    
    val referenceAllele = ColumnDefinition(
      ReferenceAllele,
      Allele2 ~> (_.toUpperCase),
      Allele1 ~> (_.toUpperCase),
      src)
      
    val effectAllele = ColumnDefinition(
      EffectAllele,
      Allele1 ~> (_.toUpperCase),
      Allele2 ~> (_.toUpperCase),
      src)
    
    val effectAllelePh = ColumnDefinition(
      EffectAllelePH,
      Allele1 ~> (_.toUpperCase),
      Allele2 ~> (_.toUpperCase),
      src)
      
    val eaf = ColumnDefinition(
      EAF,
      Freq1,
      Freq1 ~> (_.toDouble) ~> (1 - _), //TODO
      src)
      
    val eafPh = ColumnDefinition(
      EAFPH,
      Freq1,
      Freq1 ~> (_.toDouble) ~> (1 - _), //TODO
      src)
      
    val maf = ColumnDefinition(
      MAF,
      //TODO
      Freq1 ~> (_.toDouble) ~>  { 
        case x if x > 0.5 => 1.0 - x 
        case x => x 
      },
      None,
      src)
      
    val mafPh = ColumnDefinition(
      MAFPH,
      //TODO
      Freq1 ~> (_.toDouble) ~>  { 
        case x if x > 0.5 => 1.0 - x 
        case x => x 
      },
      None,
      src)
      
    val oddsRatio = ColumnDefinition(
      OddsRatio,
      Effect ~> (_.toDouble) ~> scala.math.exp,
      Effect ~> (-(_).toDouble) ~> scala.math.exp,
      src)
      
    val se = ColumnDefinition(
      SE,
      StdErr,
      StdErr,
      src)

    val pv = ColumnDefinition(
      PValue,
      PDashValue,
      PDashValue, //TODO
      src)
      
  }
  val (header, rows) = TimeUtils.time("Making DataRow iterator") {
    parse(Seq(
        ColumnDefs.varId, 
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
        ColumnDefs.pv))
  }
  
  val renderer = Renderer(CsvSource.Defaults.tabDelimitedWithHeaderCsvFormat)
  
  println(renderer.render(header))

  val s = TimeUtils.time("processing", info(_)) {
    rows.size
  }
  
  println(s)

  /*
  VAR_ID  =CHROM ."_". POS ."_". uc(Allele2) ."_". uc(Allele1)    =CHROM ."_". POS ."_". uc(Allele1) ."_". uc(Allele2)    ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  CHROM   CHROM   CHROM   ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  POS     POS     POS     ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  Reference_Allele        =uc(Allele2)    =uc(Allele1)    ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  Effect_Allele   =uc(Allele1)    =uc(Allele2)    ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  Effect_Allele_PH        =uc(Allele1)    =uc(Allele2)    ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  EAF     Freq1   =1-Freq1        ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  EAF_PH  Freq1   =1-Freq1        ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  MAF     =Freq1 > 0.5 ? 1-Freq1 : Freq1  DONOTHING       ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  MAF_PH  =Freq1 > 0.5 ? 1-Freq1 : Freq1  DONOTHING       ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  ODDS_RATIO      =exp(Effect)    =exp(-Effect)   ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  SE      StdErr  StdErr  ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  P_VALUE P-value P-value ="zcat $rawDataPrefix/humgen/diabetes2/users/mvg/portal/IFMRS/GEFOS/dv1/ALLFX_GWAS_build37.txt.gz | $path/add_CHROM_POS.pl |"
  * 
  */

  def fuse(columnDefs: Seq[ColumnDefinition]): ParseFn = {
    row => {
      val dataRowValues: Map[ColumnDefinition, String] = Map.empty ++ columnDefs.map { columnDef =>
        val columnValue = columnDef.getValueFromSource(row).toString
        
        columnDef -> columnValue
      }
    
      DataRow(dataRowValues)
    }
  }

  def parse(columnDefs: Seq[ColumnDefinition]): (HeaderRow, Iterator[DataRow]) = {
    val bySource = columnDefs.groupBy(_.source)
    
    import Maps.Implicits._
    
    val parsingFunctionsBySource = bySource.strictMapValues(fuse).toSeq
    
    val sourceHeaders = HeaderRow(columnDefs.sortBy(_.index).map(_.name.name))
    
    val isToFns = parsingFunctionsBySource.map { case (src, parseFn) => 
      (src.records, parseFn) 
    }
      
    val header = HeaderRow(columnDefs.sortBy(_.index).map(_.name.name))
    
    val rows = new Iterator[DataRow] {
      override def hasNext: Boolean = isToFns.forall { case (records, _) => records.hasNext }
      override def next(): DataRow = {
        def parseNext(t: (Iterator[CSVRecord], CSVRecord => DataRow)): DataRow = {
          val (records, parseFn) = t
          
          val record = records.next()
          
          parseFn(record)
        }
        
        isToFns.map(parseNext).foldLeft(DataRow.empty)(_ ++ _)
      }
    }
    
    (header, rows)
  }
}

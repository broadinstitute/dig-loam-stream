package loamstream.loam.intake

import java.nio.file.Paths

import loamstream.util.Loggable
import loamstream.util.TimeUtils

object Dsl extends App with Loggable {
  
  import IntakeSyntax._
  
  object ColumnDefs {
    /*
    VAR_ID	=CHROM ."_". POS ."_". uc(Allele2) ."_". uc(Allele1)	=CHROM ."_". POS ."_". uc(Allele1) ."_". uc(Allele2)	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    CHROM	CHROM	CHROM	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    POS	POS	POS	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    Reference_Allele	=uc(Allele2)	=uc(Allele1)	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    Effect_Allele	=uc(Allele1)	=uc(Allele2)	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    Effect_Allele_PH	=uc(Allele1)	=uc(Allele2)	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    EAF	Freq1	=1-Freq1	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    EAF_PH	Freq1	=1-Freq1	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    MAF	=Freq1 > 0.5 ? 1-Freq1 : Freq1	DONOTHING	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    MAF_PH	=Freq1 > 0.5 ? 1-Freq1 : Freq1	DONOTHING	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    ODDS_RATIO	=exp(Effect)	=exp(-Effect)	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    SE	StdErr	StdErr	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
    
    P_VALUE	P-value	P-value	="cat /home/clint/workspace/dig\-loam\-stream/data\.txt |"
		*/
    
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
  
  val src: CsvSource = CsvSource.FastCsv.fromCommandLine("cat data.txt")
  
  val (header, rows) = TimeUtils.time("Making DataRow iterator") {
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
  
  val renderer = CommonsCsvRenderer(CsvSource.Defaults.CommonsCsv.Formats.tabDelimitedWithHeaderCsvFormat)
  
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
  */ 
  

}

package loamstream.loam.intake

object Dsl2 {
  import IntakeSyntax._
  
  object ColumnNames {
    val VARID = "VAR_ID".asColumnName
    val CHROM = "CHROM".asColumnName
    val POS = "POS".asColumnName
    val ReferenceAllele = "Reference_Allele".asColumnName
    val EffectAllele = "Effect_Allele".asColumnName
    val EffectAllelePH = "Effect_Allele_PH".asColumnName
    val NPH = "N_PH".asColumnName
    val MAF = "MAF".asColumnName
    val MAFPH = "MAF_PH".asColumnName
    val PValue = "P_VALUE".asColumnName
    val OddsRatio = "ODDS_RATIO".asColumnName
    val SE = "SE".asColumnName
    val BEG = "BEG".asColumnName
    val MARKER_ID = "MARKER_ID".asColumnName
    val NS = "NS".asColumnName
    val PValueNoUnderscore = "PVALUE".asColumnName
    val Beta = "BETA".asColumnName 
    val SEBeta = "SEBETA".asColumnName
  }
  
  object ColumnDefs {
    import ColumnNames._
    
/*
VAR_ID	=MARKER_ID=~/^(\d+):(\d+)_([ATCG]+)\/([ATCG]+)_/ ? $1 ."_". $2 ."_". $3 ."_". $4 : next	=MARKER_ID=~/^(\d+):(\d+)_([ATCG]+)\/([ATCG]+)_/ ? $1 ."_". $2 ."_". $4 ."_". $3 : next	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
CHROM	CHROM	CHROM	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
POS	BEG	BEG	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
Reference_Allele	=MARKER_ID=~/^\d+:\d+_([ATCG]+)\/[ATCG]+_/ ? $1 : next	=MARKER_ID=~/^\d+:\d+_[ATCG]+\/([ATCG]+)_/ ? $1 : next	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
*/
    object Regexes {
      val rightFormat = """^\d+:\d+_[ATCG]+\/([ATCG]+_"""
      val all4 = """^(\d+):(\d+)_([ATCG]+)\/([ATCG]+)_"""
      val part2 = """^\d+:\d+_([ATCG]+)\/[ATCG]+_"""
      val part3 = """^\d+:\d+_[ATCG]+\/([ATCG]+)_"""
    }
    
    val getSingleMatch: Seq[String] => String = { case Seq(a) => a }
    
    val varId = {
      val notFlipped: Seq[String] => String = { case Seq(a, b, c, d) => s"${a}_${b}_${c}_${d}" }
      val ifFlipped: Seq[String] => String = { case Seq(a, b, c, d) => s"${a}_${b}_${d}_${c}" }
      
      val fromMarkerId: ColumnExpr[String] = MARKER_ID.mapRegex(Regexes.all4) {
        case Seq(a, b, c, d) => s"${a}_${b}_${c}_${d}"
      }
      
      val fromMarkerIdFlip: ColumnExpr[String] = MARKER_ID.mapRegex(Regexes.all4) {
        case Seq(a, b, c, d) => s"${a}_${b}_${d}_${c}"
      }
      
      ColumnDef(VARID, fromMarkerId, fromMarkerIdFlip)
    }
    val chrom = ColumnDef(CHROM)
    val pos = ColumnDef(POS, BEG, BEG)
    val referenceAllele = {
      val fromMarkerId = MARKER_ID.mapRegex(Regexes.part2)(getSingleMatch)
      
      val fromMarkerIdFlip = MARKER_ID.mapRegex(Regexes.part3)(getSingleMatch)
      
      ColumnDef(ReferenceAllele, fromMarkerId, fromMarkerIdFlip)
    }
    val effectAllele = {
      val fromMarkerId = MARKER_ID.mapRegex(Regexes.part3)(getSingleMatch)
      
      val fromMarkerIdFlip = MARKER_ID.mapRegex(Regexes.part2)(getSingleMatch)
      
      ColumnDef(EffectAllele, fromMarkerId, fromMarkerIdFlip)
    }
    val effectAllelePH = effectAllele.copy(name = EffectAllelePH)
    val nPH = ColumnDef(NPH, NS, NS)
    val maf = ColumnDef(MAF)
    val mafPh = ColumnDef(MAFPH, MAF, MAF)
    val pValue = ColumnDef(PValue, PValueNoUnderscore, PValueNoUnderscore)
    val oddsRatio = ColumnDef(OddsRatio, Beta.asDouble.exp, Beta.asDouble.negate.exp)
    val se = ColumnDef(SE, SEBeta, SEBeta)
    
    val source = CsvSource.FastCsv.fromCommandLine("").filter(MARKER_ID.matches(Regexes.rightFormat))
  }
  
  
    
/*
VAR_ID	=MARKER_ID=~/^(\d+):(\d+)_([ATCG]+)\/([ATCG]+)_/ ? $1 ."_". $2 ."_". $3 ."_". $4 : next	=MARKER_ID=~/^(\d+):(\d+)_([ATCG]+)\/([ATCG]+)_/ ? $1 ."_". $2 ."_". $4 ."_". $3 : next	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
CHROM	CHROM	CHROM	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
POS	BEG	BEG	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
Reference_Allele	=MARKER_ID=~/^\d+:\d+_([ATCG]+)\/[ATCG]+_/ ? $1 : next	=MARKER_ID=~/^\d+:\d+_[ATCG]+\/([ATCG]+)_/ ? $1 : next	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
Effect_Allele	=MARKER_ID=~/^\d+:\d+_[ATCG]+\/([ATCG]+)_/ ? $1 : next	=MARKER_ID=~/^\d+:\d+_([ATCG]+)\/[ATCG]+_/ ? $1 : next	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
Effect_Allele_PH	=MARKER_ID=~/^\d+:\d+_[ATCG]+\/([ATCG]+)_/ ? $1 : next	=MARKER_ID=~/^\d+:\d+_([ATCG]+)\/[ATCG]+_/ ? $1 : next	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
N_PH	NS	NS	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
MAF	MAF	MAF	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
MAF_PH	MAF	MAF	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
P_VALUE	PVALUE	PVALUE	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
ODDS_RATIO	=exp(BETA)	=exp(-BETA)	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
SE	SEBETA	SEBETA	="zcat $rawDataPrefix/humgen/diabetes/users/ryank/data/camp/analysis/stats/T2D.epacts.emmax/T2D.epacts.emmax.gz | awk '\$11 != \"NA\"' |"
   */
}

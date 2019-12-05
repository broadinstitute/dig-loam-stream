package loamstream.loam.intake

import loamstream.loam.LoamSyntax
import loamstream.loam.LoamScriptContext
import loamstream.conf.DataConfig

final class Intake {
  import LoamSyntax._

  implicit val scriptContext: LoamScriptContext = ???

  object Executables {
    val runWithConfigs = path("bin/run_with_configs.pl")

    val uploadDataTables = path("bin/upload_data_tables.pl")

    val generateMetadata = path("bin/generate_metadata.pl")

    val uploadMetadataTables = path("bin/upload_metadata_tables.pl")

    val generateIndices = path("bin/generate_indices.pl")
  }

  lazy val config: DataConfig = loadConfig("intake.conf")

  object Paths {
    val outputDir = path(config.getStr("output-dir"))

    val intakeConfigFile = path(config.getStr("config-file"))
  }

  object Regexes {
    val levelDatasetAndPhenotype = "(var|gen)Data\\.(.+?)\\.(.+?)\\.txt".r
  }

  def extractLevelDatasetAndPhenotypeFromConfigFileName(fileName: String): Option[(Level, String, String)] = {
    fileName match {
      //varData.GWAS_MAGIC_eu_dv6.HBA1C.txt
      case Regexes.levelDatasetAndPhenotype(l, dataset, phenotype) => {
        for {
          level <- Level.fromString(l)
        } yield {
          (level, dataset.trim, phenotype.trim)
        }
      }
      case _ => None
    }
  }

  sealed trait Level
  
  object Level {
    case object Gene extends Level
    case object Variant extends Level
    
    def fromString(s: String): Option[Level] = s.toUpperCase match {
      case "GEN" => Some(Gene)
      case "VAR" => Some(Variant)
      case _ => None
    }
  }
  
  object Stores {
    def forRunWithConfigs(level: Level, dataSetName: String, phenotype: String): Iterable[Store] = {
      val levelPrefix: String = level match {
        case Level.Gene => "GEN_"
        case _ => ""
      }
      
      val commonDir = Paths.outputDir / s"COMMON"
      val datasetsDir = Paths.outputDir / s"DATASETS"
      val phenotypesDir = Paths.outputDir / s"PHENOTYPES"

      val dataFile = s"${levelPrefix}${dataSetName}.${phenotype}.1.txt"
      val md5File = s"${levelPrefix}${dataFile}.md5"
      val dataListFile = s"${levelPrefix}data.list"
      val schemaListFile = s"${levelPrefix}schema.list"
      val schemaFile = s"${levelPrefix}${dataSetName}.${phenotype}.1_schema.txt"
      
      def filesIn(baseDir: Path): Seq[Path] = Seq(
        baseDir / dataSetName / phenotype / "DATA" / dataFile,
        baseDir / dataSetName / phenotype / "DATA" / md5File,
        baseDir / dataSetName / phenotype / "LIST" / dataListFile,
        baseDir / dataSetName / phenotype / "LIST" / schemaListFile,
        baseDir / dataSetName / phenotype / "SCHEMA" / schemaFile)

      val paths: Seq[Path] = filesIn(commonDir) ++ filesIn(datasetsDir) ++ filesIn(phenotypesDir)

      paths.map(store(_))
    }
    
    def forGenerateMetadata: Iterable[Store] = {
      val types = Seq(
          "DATASET",
          "DATASET_PH",
          "DATASET_PH_schema",
          "DATASET_schema",
          "MDV",
          "MDV_schema",
          "PH",
          "PH_schema",
          "PROP",
          "PROP_DATASET",
          "PROP_DATASET_PH",
          "PROP_DATASET_PH_schema",
          "PROP_DATASET_schema",
          "PROP_schema")

      val extensions = Seq("list", "txt")
          
      val txtAndListFileNames = for {
        tpe <- types
        ext <- extensions
      } yield {
        s"meta_data_file.ver4-META_${tpe}.${ext}"
      }
      
      val toIndexFileNames = Seq("metadata_to_index.ver4.txt", "tables_to_index.ver4.txt")
      
      (toIndexFileNames ++ txtAndListFileNames).map(store(_))
    }

    val intakeConfigFile = store(Paths.intakeConfigFile)
  }

  def commands(intakeConfigFileName: String): Unit = {
    //TODO
    val Some((level, datasetName, phenotype)) = extractLevelDatasetAndPhenotypeFromConfigFileName(intakeConfigFileName)

    def runWithConfigs(): Unit = {
      val outputs = Stores.forRunWithConfigs(level, datasetName, phenotype)

      import Stores.intakeConfigFile

      cmd"perl ${Executables.runWithConfigs} ${intakeConfigFile}".in(intakeConfigFile).out(outputs)
    }

    def uploadDataTables(): Unit = {
      //perl upload_data_tables.pl DATASETS/GWAS_MAGIC_eu_dv6/HBA1C PHENOTYPES/GWAS_MAGIC_eu_dv6/HBA1C

      cmd"perl ${Executables.uploadDataTables} DATASETS/${datasetName}/${phenotype} PHENOTYPES/${datasetName}/${phenotype}".in(???, ???)
    }

    def generateMetadata(): Unit = {
      val ruleFile = store("meta_data_file-RULE.txt")
      val phFile = store("meta_data_file-PH.txt")

      val outputs: Iterable[Store] = Stores.forGenerateMetadata

      cmd"perl ${Executables.generateMetadata}".in(ruleFile, phFile).out(outputs)
    }

    runWithConfigs()

    uploadDataTables()

    generateMetadata()
  }
  
  commands(Paths.intakeConfigFile.getFileName.toString)
}

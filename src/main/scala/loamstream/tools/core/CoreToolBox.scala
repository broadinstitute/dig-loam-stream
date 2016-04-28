package loamstream.tools.core

import java.nio.file.{Files, Path}

import scala.concurrent.{ ExecutionContext, Future, blocking }

import CoreToolBox._
import htsjdk.variant.variantcontext.Genotype
import loamstream.LEnv
import loamstream.model.LPipeline
import loamstream.model.Tool
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.{LCommandLineJob, LJob, LToolBox}
import loamstream.model.jobs.LJob.{Result, SimpleFailure, SimpleSuccess}
import loamstream.model.kinds.LSpecificKind
import loamstream.tools.{HailTools, PcaProjecter, PcaWeightsReader, VcfParser}
import loamstream.tools.VcfUtils
import loamstream.tools.klusta.{KlustaKwikKonfig, KlustaKwikLineCommand}
import loamstream.tools.klusta.{KlustaKwikLineCommand, KlustaKwikInputWriter}
import loamstream.util.{Hit, Miss, Shot}
import loamstream.util.Functions
import loamstream.util.LoamFileUtils
import loamstream.util.SnagMessage

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
object CoreToolBox {

  case class FileExists(path: Path) extends LJob.Success {
    override def successMessage: String = path + " exists"
  }


  trait CheckPreexistingFileJob extends LJob {
    def file: Path

    override def inputs: Set[LJob] = Set.empty

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        if (Files.exists(file)) {
          FileExists(file)
        } else {
          SimpleFailure(s"$file does not exist.")
        }
      }
    }
  }

  case class CheckPreexistingVcfFileJob(file: Path) extends CheckPreexistingFileJob

  case class CheckPreexistingPcaWeightsFileJob(file: Path) extends CheckPreexistingFileJob

  case class ExtractSampleIdsFromVcfFileJob(vcfFileJob: CheckPreexistingVcfFileJob, samplesFile: Path) extends LJob {

    override def inputs: Set[LJob] = Set(vcfFileJob)

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        val samples = VcfParser(vcfFileJob.file).samples
        LoamFileUtils.printToFile(samplesFile.toFile) {
          p => samples.foreach(p.println) // scalastyle:ignore
        }
        new SimpleSuccess("Extracted sample ids.")
      }
    }
  }

  case class ImportVcfFileJob(vcfFileJob: CheckPreexistingVcfFileJob, vdsFile: Path) extends LJob {

    override def inputs: Set[LJob] = Set(vcfFileJob)

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        HailTools.importVcf(vcfFileJob.file, vdsFile)
        SimpleSuccess("Imported VCF in VDS format.")
      }
    }
  }

  case class CalculateSingletonsJob(importVcfFileJob: ImportVcfFileJob, singletonsFile: Path) extends LJob {

    override def inputs: Set[LJob] = Set(importVcfFileJob)

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        HailTools.calculateSingletons(importVcfFileJob.vdsFile, singletonsFile)
        new SimpleSuccess("Calculated singletons from VDS.")
      }
    }
  }

  case class CalculatePcaProjectionsJob(vcfFileJob: CheckPreexistingVcfFileJob,
                                        pcaWeightsJob: CheckPreexistingPcaWeightsFileJob,
                                        klustaKwikKonfig: KlustaKwikKonfig)
    extends LJob {
    override def inputs: Set[LJob] = Set(vcfFileJob, pcaWeightsJob)

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        blocking {
          val weights = PcaWeightsReader.read(pcaWeightsJob.file)
          val pcaProjecter = PcaProjecter(weights)
          val vcfParser = VcfParser(vcfFileJob.file)
          val samples = vcfParser.samples
          val genotypeToDouble: Genotype => Double = { genotype => VcfUtils.genotypeToAltCount(genotype).toDouble }
          val pcaProjections = pcaProjecter.project(samples, vcfParser.genotypeMapIter, genotypeToDouble)
          KlustaKwikInputWriter.writeFeatures(klustaKwikKonfig, pcaProjections)
          new SimpleSuccess(s"Wrote PCA projections to file ${klustaKwikKonfig.inputFile}")
        }
      }
    }
  }
}

case class CoreToolBox(env: LEnv) extends LToolBox {
  val stores = CoreStore.stores
  val tools = CoreTool.tools(env)

  lazy val genotypesId = env(LCoreEnv.Keys.genotypesId)
  lazy val checkPreexistingVcfFileTool = CoreTool.checkPreExistingVcfFile(genotypesId)
  
  //TODO: Why does this make tests crash?
  //lazy val pcaWeightsId = env(LCoreEnv.Keys.pcaWeightsId)  
  //lazy val checkPreexistingPcaWeightsFileTool = CoreTool.checkPreExistingPcaWeightsFile(pcaWeightsId)

  val predefinedVcfFileShot: String => Shot[Path] = Functions.memoize { id =>
    env.shoot(LCoreEnv.Keys.vcfFilePath).map(_ (id))
  }

  lazy val sampleFileShot: Shot[Path] = env.shoot(LCoreEnv.Keys.sampleFilePath).map(_.get)

  lazy val singletonFileShot: Shot[Path] = env.shoot(LCoreEnv.Keys.singletonFilePath).map(_.get)

  lazy val pcaWeightsFileShot: Shot[Path] = env.shoot(LCoreEnv.Keys.pcaWeightsFilePath).map(_.get)

  lazy val klustaKwikConfigShot: Shot[KlustaKwikKonfig] = env.shoot(LCoreEnv.Keys.klustaKwikKonfig)

  lazy val vcfFileJobShot: Shot[CheckPreexistingVcfFileJob] = {
    checkPreexistingVcfFileTool.spec.kind match {
      case LSpecificKind(specifics, _) => specifics.value match {
        case (_, id: String) => predefinedVcfFileShot(id).map(CheckPreexistingVcfFileJob)
        case _ => Miss(SnagMessage("Tool is not of the right kind."))
      }
      case _ => Miss(SnagMessage("Can't get id for VCF file."))
    }
  }

  lazy val extractSamplesJobShot: Shot[ExtractSampleIdsFromVcfFileJob] = {
    (vcfFileJobShot and sampleFileShot) (ExtractSampleIdsFromVcfFileJob)
  }

  lazy val importVcfJobShot: Shot[ImportVcfFileJob] = {
    (vcfFileJobShot and sampleFileShot) (ImportVcfFileJob)
  }

  lazy val calculateSingletonsJobShot: Shot[CalculateSingletonsJob] = {
    (importVcfJobShot and singletonFileShot) (CalculateSingletonsJob)
  }

  lazy val pcaWeightsFileJobShot: Shot[CheckPreexistingPcaWeightsFileJob] = {
    pcaWeightsFileShot.map(CheckPreexistingPcaWeightsFileJob)
  }

  lazy val calculatePcaProjectionsJobShot: Shot[CalculatePcaProjectionsJob] = {
    (vcfFileJobShot and pcaWeightsFileJobShot and klustaKwikConfigShot) (CalculatePcaProjectionsJob)
  }

  lazy val calculateClustersJobShot: Shot[LCommandLineJob] = {
    (calculatePcaProjectionsJobShot and klustaKwikConfigShot) { (calculatePcaProjectionJob, klustaKwikKonfig) =>
      val commandLine = KlustaKwikLineCommand.klustaKwik(klustaKwikKonfig)
      LCommandLineJob(commandLine, klustaKwikKonfig.workDir, Set(calculatePcaProjectionJob))
    }
  }

  def toolToJobShot(tool: Tool): Shot[LJob] = tool match {
    case this.checkPreexistingVcfFileTool => vcfFileJobShot
    case CoreTool.extractSampleIdsFromVcfFile => extractSamplesJobShot
    case CoreTool.importVcf => importVcfJobShot
    case CoreTool.calculateSingletons => calculateSingletonsJobShot
    //TODO: Why does this make tests crash?
    //case this.checkPreexistingPcaWeightsFileTool => pcaWeightsFileJobShot
    case CoreTool.projectPcaNative => calculatePcaProjectionsJobShot
    case CoreTool.klustaKwikClustering => calculateClustersJobShot
    case CoreTool.projectPca => calculatePcaProjectionsJobShot
    case CoreTool.clusteringSamplesByFeatures => calculateClustersJobShot
    case _ => Miss(SnagMessage(s"Have not yet implemented tool $tool"))
  }

  override def createJobs(tool: Tool, pipeline: LPipeline): Shot[Set[LJob]] = {
    toolToJobShot(tool).map(Set(_))
  }

  override def createExecutable(pipeline: LPipeline): LExecutable = {
    //TODO: Mapping over a property of a pipeline with a function that takes the pipeline feels weird.
    LExecutable(pipeline.tools.map(createJobs(_, pipeline)).collect { case Hit(job) => job }.flatten)
  }
}

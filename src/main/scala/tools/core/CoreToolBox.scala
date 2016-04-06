package tools.core

import java.nio.file.{Files, Path}

import htsjdk.variant.variantcontext.Genotype
import loamstream.LEnv
import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LJob.{Result, SimpleFailure, SimpleSuccess}
import loamstream.model.jobs.tools.LTool
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.model.kinds.LSpecificKind
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import loamstream.util.shot.{Hit, Miss, Shot}
import loamstream.util.snag.SnagMessage
import tools.core.CoreToolBox._
import tools.{HailTools, KlustaKwikInputWriter, PcaProjecter, PcaWeightsReader, VcfParser, VcfUtils}
import utils.LoamFileUtils

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

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
          SimpleFailure(file.toString + " does not exist.")
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
        new SimpleSuccess("Imported VCF in VDS format.")
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
                                        pcaProjectionsFile: Path) extends LJob {
    override def inputs: Set[LJob] = Set(vcfFileJob, pcaWeightsJob)

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        val weights = PcaWeightsReader.read(pcaWeightsJob.file)
        val pcaProjecter = PcaProjecter(weights)
        val vcfParser = VcfParser(vcfFileJob.file)
        val samples = vcfParser.samples
        val genotypeToDouble: Genotype => Double = { genotype => VcfUtils.genotypeToAltCount(genotype).toDouble }
        val pcaProjections = pcaProjecter.project(samples, vcfParser.genotypeMapIter, genotypeToDouble)
        KlustaKwikInputWriter.writeFeatures(pcaProjectionsFile, pcaProjections)
        new SimpleSuccess("Wrote PCA projections to file " + pcaProjectionsFile)
      }
    }
  }

}

case class CoreToolBox(env: LEnv) extends LToolBox {
  val stores = CoreStore.stores
  val tools = CoreTool.tools(env)

  lazy val genotypesId = env(LCoreEnv.Keys.genotypesId)
  lazy val checkPreexistingVcfFileTool = CoreTool.checkPreExistingVcfFile(genotypesId)
  lazy val pcaWeightsId = env(LCoreEnv.Keys.pcaWeightsId)
  lazy val checkPreexistingPcaWeightsFileTool = CoreTool.checkPreExistingPcaWeightsFile(pcaWeightsId)

  var vcfFiles: Map[String, Path] = Map.empty

  override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile.spec)

  override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe.spec)

  def getPredefinedVcfFile(id: String): Path = {
    vcfFiles.get(id) match {
      case Some(path) => path
      case None =>
        val path = env(LCoreEnv.Keys.vcfFilePath)(id)
        vcfFiles += (id -> path)
        path
    }
  }

  lazy val getSampleFile: Path = env(LCoreEnv.Keys.sampleFilePath).get

  lazy val getSingletonFile: Path = env(LCoreEnv.Keys.singletonFilePath).get

  lazy val getPcaWeightsFile: Path = env(LCoreEnv.Keys.pcaWeightsFilePath).get

  lazy val getPcaProjectionsFile: Path = env(LCoreEnv.Keys.pcaProjectionsFilePath).get

  lazy val getClusterFile: Path = env(LCoreEnv.Keys.clusterFilePath).get

  def createVcfFileJob: Shot[CheckPreexistingVcfFileJob] = {
    checkPreexistingVcfFileTool.recipe.kind match {
      case LSpecificKind(specifics, _) => specifics match {
        case (_, id: String) => Hit(CheckPreexistingVcfFileJob(getPredefinedVcfFile(id)))
        case _ => Miss(SnagMessage("Recipe is not of the right kind."))
      }
      case _ => Miss(SnagMessage("Can't get id for VCF file."))
    }
  }

  def createExtractSamplesJob: Shot[ExtractSampleIdsFromVcfFileJob] =
    createVcfFileJob.map(ExtractSampleIdsFromVcfFileJob(_, getSampleFile))

  def createImportVcfJob: Shot[ImportVcfFileJob] =
    createVcfFileJob.map(ImportVcfFileJob(_, getSampleFile))

  def calculateSingletonsJob: Shot[CalculateSingletonsJob] =
    createImportVcfJob.map(CalculateSingletonsJob(_, getSingletonFile))

  def createPcaWeightsJob: Shot[CheckPreexistingPcaWeightsFileJob] =
    Shot.fromTry(Try {
      CheckPreexistingPcaWeightsFileJob(getPcaWeightsFile)
    })

  override def createJobs(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): Shot[Set[LJob]] = {
    mapping.tools.get(recipe) match {
      case Some(tool) => tool match {
        case this.checkPreexistingVcfFileTool => createVcfFileJob.map(Set(_))
        case CoreTool.extractSampleIdsFromVcfFile => createExtractSamplesJob.map(Set(_))
        case CoreTool.importVcf => createImportVcfJob.map(Set(_))
        case CoreTool.calculateSingletons => calculateSingletonsJob.map(Set(_))
        case this.checkPreexistingPcaWeightsFileTool => createPcaWeightsJob.map(Set(_))
        case _ => Miss(SnagMessage("Have not yet implemented tool " + tool))
      }
      case None => Miss(SnagMessage("No tool mapped to recipe " + recipe))
    }
  }

  override def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable = {
    LExecutable(mapping.tools.keySet.map(createJobs(_, pipeline, mapping)).collect({ case Hit(job) => job }).flatten)
  }
}

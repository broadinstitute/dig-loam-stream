package tools.core

import java.nio.file.{Files, Path}

import loamstream.apps.minimal.MiniPipeline
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
import tools.{HailTools, VcfParser}
import utils.LoamFileUtils

import scala.concurrent.{ExecutionContext, Future}

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
object CoreToolBox {

  case class VcfFileExists(path: Path) extends LJob.Success {
    override def successMessage: String = path + " exists"
  }

  case class CheckPreexistingVcfFileJob(vcfFile: Path) extends LJob {
    override def inputs: Set[LJob] = Set.empty

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        if (Files.exists(vcfFile)) {
          VcfFileExists(vcfFile)
        } else {
          SimpleFailure(vcfFile.toString + " does not exist.")
        }
      }
    }
  }

  case class ExtractSampleIdsFromVcfFileJob(vcfFileJob: CheckPreexistingVcfFileJob, samplesFile: Path) extends LJob {

    override def inputs: Set[LJob] = Set(vcfFileJob)

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        val samples = VcfParser(vcfFileJob.vcfFile).samples
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
        HailTools.importVcf(vcfFileJob.vcfFile, vdsFile)
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

}

case class CoreToolBox(config: CoreConfig) extends LToolBox {
  val stores = CoreStore.stores
  val genotypesId = MiniPipeline.genotypeCallsPileId
  val tools = CoreTool.tools(genotypesId)

  val checkPreexistingVcfFileTool = CoreTool.checkPreExistingVcfFile(genotypesId)

  var vcfFiles: Map[String, Path] = Map.empty

  override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile.spec)

  override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe.spec)

  def getPredefinedVcfFile(id: String): Path = {
    vcfFiles.get(id) match {
      case Some(path) => path
      case None =>
        val path = config.getVcfFilePathFun(id)
        vcfFiles += (id -> path)
        path
    }
  }

  lazy val getSampleFile: Path = config.getSampleFilePathFun.get

  lazy val getSingletonFile: Path = config.getSingletonFilePathFun.get

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

  override def createJobs(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): Shot[Set[LJob]] = {
    mapping.tools.get(recipe) match {
      case Some(tool) => tool match {
        case this.checkPreexistingVcfFileTool => createVcfFileJob.map(Set(_))
        case CoreTool.extractSampleIdsFromVcfFile => createExtractSamplesJob.map(Set(_))
        case CoreTool.importVcf => createImportVcfJob.map(Set(_))
        case CoreTool.calculateSingletons => calculateSingletonsJob.map(Set(_))
        case _ => Miss(SnagMessage("Have not yet implemented tool " + tool))
      }
      case None => Miss(SnagMessage("No tool mapped to recipe " + recipe))
    }
  }

  override def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable = {
    LExecutable(mapping.tools.keySet.map(createJobs(_, pipeline, mapping)).collect({ case Hit(job) => job }).flatten)
  }
}

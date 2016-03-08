package loamstream.apps.minimal

import java.nio.file.{Files, Path}

import loamstream.apps.minimal.MiniToolBox.{CheckPreexistingVcfFileJob, Config, ExtractSampleIdsFromVcfFileJob}
import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.{Result, SimpleFailure, SimpleSuccess}
import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.LSpecificKind
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import loamstream.util.FileAsker
import loamstream.util.shot.{Hit, Miss, Shot}
import loamstream.util.snag.SnagAtom
import tools.VcfParser
import utils.Loggable

import scala.concurrent.{ExecutionContext, Future}

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
object MiniToolBox extends Loggable {

  trait Config {
    def getVcfFilePath(id: String): Path

    def getSampleFilePath: Path
  }

  object InteractiveConfig extends Config {
    override def getVcfFilePath(id: String): Path = FileAsker.ask("VCF file '" + id + "'")

    override def getSampleFilePath: Path = FileAsker.ask("samples file")
  }

  case class InteractiveFallbackConfig(vcfFiles: Seq[String => Path], sampleFiles: Seq[Path]) extends Config {
    override def getVcfFilePath(id: String): Path =
      FileAsker.askIfNotExist(vcfFiles.map(_ (id)))("VCF file '" + id + "'")

    override def getSampleFilePath: Path = FileAsker.askIfParentDoesNotExist(sampleFiles)("samples file")
  }

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
    val vcfParser = new VcfParser

    override def inputs: Set[LJob] = Set(vcfFileJob)

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        val samples = vcfParser.readSamples(vcfFileJob.vcfFile)
        vcfParser.printToFile(samplesFile.toFile) {
          p => samples.foreach(p.println) // scalastyle:ignore
        }
        new SimpleSuccess("Extracted sample ids.")
      }
    }
  }

}

case class MiniToolBox(config: Config) extends LBasicToolBox {
  val stores = MiniStore.stores
  val tools = MiniTool.tools

  var vcfFiles: Map[String, Path] = Map.empty
  var sampleFileOpt: Option[Path] = None

  override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile.spec)

  override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe.spec)

  override def getPredefindedVcfFile(id: String): Path = {
    vcfFiles.get(id) match {
      case Some(path) => path
      case None =>
        val path = config.getVcfFilePath(id)
        vcfFiles += (id -> path)
        path
    }
  }

  override def getSampleFile: Path = {
    sampleFileOpt match {
      case Some(path) => path
      case None =>
        val path = config.getSampleFilePath
        sampleFileOpt = Some(path)
        path
    }
  }

  def createVcfFileJob: Shot[CheckPreexistingVcfFileJob] = {
    MiniTool.checkPreExistingVcfFile.recipe.kind match {
      case LSpecificKind(specifics, _) => specifics match {
        case (_, id: String) => Hit(CheckPreexistingVcfFileJob(getPredefindedVcfFile(id)))
        case _ => Miss(SnagAtom("Recipe is not of the right kind."))
      }
      case _ => Miss(SnagAtom("Can't get id for VCF file."))
    }
  }

  def createExtractSamplesJob: Shot[ExtractSampleIdsFromVcfFileJob] =
    createVcfFileJob.map(ExtractSampleIdsFromVcfFileJob(_, getSampleFile))


  override def createJobs(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): Shot[Set[LJob]] = {
    mapping.tools.get(recipe) match {
      case Some(tool) => tool match {
        case MiniTool.checkPreExistingVcfFile => createVcfFileJob.map(Set(_))
        case MiniTool.extractSampleIdsFromVcfFile => createExtractSamplesJob.map(Set(_))
        case _ => Miss(SnagAtom("Have not yet implemented tool " + tool))
      }
      case None => Miss(SnagAtom("No tool mapped to recipe " + recipe))
    }
  }

  override def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable = {
    LExecutable(mapping.tools.keySet.map(createJobs(_, pipeline, mapping)).collect({ case Hit(job) => job }).flatten)
  }
}

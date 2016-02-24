package loamstream.apps.minimal

import java.io.{BufferedReader, FileReader}
import java.nio.file.{Files, Path}

import loamstream.apps.minimal.MiniToolBox.{CheckPreexistingVcfFileJob, Config, ExtractSampleIdsFromVcfFileJob}
import loamstream.map.LToolMapping
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.LJob
import loamstream.model.jobs.LJob.Result
import loamstream.model.jobs.tools.LTool
import loamstream.model.kinds.LSpecificKind
import loamstream.model.piles.LPile
import loamstream.model.recipes.LRecipe
import loamstream.model.stores.LStore
import tools.VcfParser
import util.shot.{Hit, Miss, Shot}
import util.snag.SnagAtom

import scala.concurrent.{ExecutionContext, Future}

/**
 * LoamStream
 * Created by oliverr on 2/23/2016.
 */
object MiniToolBox {

  case class Config(dataFilesFolder: Path)

  case class VcfFileExists(path: Path) extends LJob.Success {
    override def successMessage: String = path + " exists"
  }

  case class CheckPreexistingVcfFileJob(vcfFile: Path) extends LJob {
    override def inputs: Set[LJob] = Set.empty

    override def execute(implicit context: ExecutionContext): Shot[Future[Result]] = {
      if (Files.exists(vcfFile)) {
        Hit(Future {
          VcfFileExists(vcfFile)
        })
      } else {
        Miss(SnagAtom(vcfFile + " does not exist."))
      }
    }
  }

  case class ExtractSampleIdsFromVcfFileJob(vcfFileJob: CheckPreexistingVcfFileJob, samplesFile: Path) extends LJob {
    val vcfParser = new VcfParser

    override def inputs: Set[LJob] = Set(vcfFileJob)

    override def execute(implicit context: ExecutionContext): Shot[Future[Result]] = {
      Hit(Future {
        val headerLine = vcfParser.getHeaderLine(new BufferedReader(new FileReader(vcfFileJob.vcfFile.toFile)))
        val samples = vcfParser.getSamples(headerLine)
        vcfParser.printToFile(samplesFile.toFile) {
          p => samples.foreach(p.println)
        }
        new LJob.Success {
          val successMessage: String = "Extracted sample ids."
        }
      })
    }
  }

}

case class MiniToolBox(config: Config) extends LBasicToolBox {
  val vcfFilesFolder = config.dataFilesFolder.resolve("vcf")
  val sampleFilesFolder = config.dataFilesFolder.resolve("samples")
  val stores = MiniStore.stores
  val tools = MiniTool.tools

  override def storesFor(pile: LPile): Set[LStore] = stores.filter(_.pile <:< pile)

  override def toolsFor(recipe: LRecipe): Set[LTool] = tools.filter(_.recipe <<< recipe)

  override def getPredefindedVcfFile(id: String): Path = vcfFilesFolder.resolve(id + ".vcf")

  override def pickNewSampleFile: Path = sampleFilesFolder.resolve("samples" + System.currentTimeMillis())

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
    createVcfFileJob.map(ExtractSampleIdsFromVcfFileJob(_, pickNewSampleFile))


  override def createJob(recipe: LRecipe, pipeline: LPipeline, mapping: LToolMapping): Shot[LJob] = {
    mapping.tools.get(recipe) match {
      case Some(tool) => tool match {
        case MiniTool.checkPreExistingVcfFile => createVcfFileJob
        case MiniTool.extractSampleIdsFromVcfFile => createExtractSamplesJob
        case _ => Miss(SnagAtom("Have not yet implemented tool " + tool))
      }
      case None => Miss(SnagAtom("No tool mapped to recipe " + recipe))
    }
  }

  override def createExecutable(pipeline: LPipeline, mapping: LToolMapping): LExecutable = {
    LExecutable(mapping.tools.keySet.map(createJob(_, pipeline, mapping)).collect({ case Hit(job) => job }))
  }
}

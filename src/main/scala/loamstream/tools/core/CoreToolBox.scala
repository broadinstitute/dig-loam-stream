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
import loamstream.tools.{HailTools, PcaProjecter, PcaWeightsReader, VcfParser}
import loamstream.tools.VcfUtils
import loamstream.tools.klusta.{KlustaKwikKonfig, KlustaKwikLineCommand}
import loamstream.tools.klusta.{KlustaKwikLineCommand, KlustaKwikInputWriter}
import loamstream.util.{Hit, Miss, Shot}
import loamstream.util.Functions
import loamstream.util.LoamFileUtils
import loamstream.util.SnagMessage
import loamstream.model.AST
import loamstream.model.AST.ToolNode
import loamstream.model.LId
import loamstream.model.ToolSpec
import loamstream.model.Store

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
object CoreToolBox {

  final case class FileExists(path: Path) extends LJob.Success {
    override def successMessage: String = s"'$path' exists"
  }

  trait CheckPreexistingFileJob extends LJob {
    def file: Path

    override def execute(implicit context: ExecutionContext): Future[Result] = {
      Future {
        if (Files.exists(file)) { FileExists(file) } 
        else { SimpleFailure(s"$file does not exist.") }
      }
    }
  }

  final case class CheckPreexistingVcfFileJob(file: Path, inputs: Set[LJob] = Set.empty) extends CheckPreexistingFileJob {
    @deprecated("", "")
    override def toString = s"CheckPreexistingVcfFileJob($file, ...)"
    
    override def withInputs(newInputs: Set[LJob]) : LJob = copy(inputs = newInputs)
  }

  final case class CheckPreexistingPcaWeightsFileJob(file: Path, inputs: Set[LJob] = Set.empty) extends CheckPreexistingFileJob {
    @deprecated("", "")
    override def toString = s"CheckPreexistingPcaWeightsFileJob($file, ...)"
    
    override def withInputs(newInputs: Set[LJob]) : LJob = copy(inputs = newInputs)
  }

  final case class ExtractSampleIdsFromVcfFileJob(vcfFile: Path, samplesFile: Path, inputs: Set[LJob] = Set.empty) extends LJob with LJob.Helpers {

    override def withInputs(newInputs: Set[LJob]) : LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      val samples = VcfParser(vcfFile).samples
          
      LoamFileUtils.printToFile(samplesFile.toFile) {
        p => samples.foreach(p.println) // scalastyle:ignore
      }
          
      SimpleSuccess("Extracted sample ids.")
    }
  }

  final case class ImportVcfFileJob(vcfFile: Path, vdsFile: Path, inputs: Set[LJob] = Set.empty) extends LJob with LJob.Helpers {

    override def withInputs(newInputs: Set[LJob]) : LJob = copy(inputs = newInputs)
    
    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      HailTools.importVcf(vcfFile, vdsFile)
        
      SimpleSuccess("Imported VCF in VDS format.")
    }
  }

  final case class CalculateSingletonsJob(vdsDir: Path, singletonsFile: Path, inputs: Set[LJob] = Set.empty) extends LJob with LJob.Helpers {

    override def withInputs(newInputs: Set[LJob]) : LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      HailTools.calculateSingletons(vdsDir, singletonsFile)
    
      SimpleSuccess("Calculated singletons from VDS.")
    }
  }

  final case class CalculatePcaProjectionsJob(vcfFile: Path,
                                              pcaWeightsFile: Path,
                                              klustaKwikKonfig: KlustaKwikKonfig,
                                              inputs: Set[LJob] = Set.empty) extends LJob with LJob.Helpers {
    
    @deprecated("", "")
    override def toString = s"CalculatePcaProjectionsJob($vcfFile, $pcaWeightsFile, ...)"
    
    override def withInputs(newInputs: Set[LJob]) : LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      val weights = PcaWeightsReader.read(pcaWeightsFile)
      val pcaProjecter = PcaProjecter(weights)
      val vcfParser = VcfParser(vcfFile)
      val samples = vcfParser.samples
      val genotypeToDouble: Genotype => Double = { genotype => VcfUtils.genotypeToAltCount(genotype).toDouble }
      val pcaProjections = pcaProjecter.project(samples, vcfParser.genotypeMapIter, genotypeToDouble)
      
      KlustaKwikInputWriter.writeFeatures(klustaKwikKonfig.inputFile, pcaProjections)
      
      SimpleSuccess(s"Wrote PCA projections to file ${klustaKwikKonfig.inputFile}")
    }
  }
}

final case class CoreToolBox(env: LEnv) extends LToolBox {

  private def pathShot(path: Path): Shot[Path] = {
    if(path.toFile.exists) { Hit(path) } 
    else { Miss(s"Couldn't find '$path'") }
  }
  
  def sampleFileShot(path: Path): Shot[Path] = pathShot(path)

  def vcfFileJobShot(path: Path): Shot[CheckPreexistingVcfFileJob] = Hit(CheckPreexistingVcfFileJob(path))

  def pcaWeightsFileJobShot(path: Path): Shot[CheckPreexistingPcaWeightsFileJob] = {
    Hit(CheckPreexistingPcaWeightsFileJob(path))
  }
  
  def extractSamplesJobShot(vcfFile: Path, sampleFile: Path): Shot[ExtractSampleIdsFromVcfFileJob] = {
    for {
      verifiedVcfFile <- pathShot(vcfFile)
    } yield {
      ExtractSampleIdsFromVcfFileJob(verifiedVcfFile, sampleFile)
    }
  }

  def convertVcfToVdsJobShot(vcfFile: Path, vdsPath: Path): Shot[ImportVcfFileJob] = {
    for {
      verifiedVcfFile <- pathShot(vcfFile)
    } yield {
      ImportVcfFileJob(verifiedVcfFile, vdsPath)
    }
  }

  def calculateSingletonsJobShot(vdsDir: Path, singletonsFile: Path): Shot[CalculateSingletonsJob] = {
    for {
      verifiedVdsDir <- pathShot(vdsDir)
    } yield {
      CalculateSingletonsJob(verifiedVdsDir, singletonsFile)
    }
  }

  def calculatePcaProjectionsJobShot(vcfFile: Path, pcaWeightsFile: Path, klustaConfig: KlustaKwikKonfig): Shot[CalculatePcaProjectionsJob] = {
    for {
      verifiedVcfFile <- pathShot(vcfFile)
      verifiedPcaWeightsFile <- pathShot(pcaWeightsFile)
    } yield {
      CalculatePcaProjectionsJob(verifiedVcfFile, verifiedPcaWeightsFile, klustaConfig)
    }
  }

  def calculateClustersJobShot(klustaConfig: KlustaKwikKonfig): Shot[LCommandLineJob] = {
    Hit {
      val commandLine = KlustaKwikLineCommand.klustaKwik(klustaConfig)
      
      LCommandLineJob(commandLine, klustaConfig.workDir, Set.empty)
    }
  }

  def toolToJobShot(tool: Tool): Shot[LJob] = tool match {
    case CoreTool.CheckPreExistingVcfFile(vcfFile) => vcfFileJobShot(vcfFile)
    case CoreTool.CheckPreExistingPcaWeightsFile(pcaWeightsFile) => pcaWeightsFileJobShot(pcaWeightsFile)
    case CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleFile) => extractSamplesJobShot(vcfFile, sampleFile)
    case CoreTool.ConvertVcfToVds(vcfFile, vdsDir) => convertVcfToVdsJobShot(vcfFile, vdsDir)
    case CoreTool.CalculateSingletons(vdsDir, singletonsFile) => calculateSingletonsJobShot(vdsDir, singletonsFile)
    case CoreTool.ProjectPcaNative(vcfFile, pcaWeightsFile, klustaKonfig) => calculatePcaProjectionsJobShot(vcfFile, pcaWeightsFile, klustaKonfig)
    case CoreTool.ProjectPca(vcfFile, pcaWeightsFile, klustaKonfig) => calculatePcaProjectionsJobShot(vcfFile, pcaWeightsFile, klustaKonfig)
    case CoreTool.KlustaKwikClustering(klustaConfig) => calculateClustersJobShot(klustaConfig)
    case CoreTool.ClusteringSamplesByFeatures(klustaConfig) => calculateClustersJobShot(klustaConfig)
    case _ => Miss(SnagMessage(s"Have not yet implemented tool $tool"))
  }

  override def createJobs(tool: Tool, pipeline: LPipeline): Shot[Set[LJob]] = {
    toolToJobShot(tool).map(Set(_))
  }

  override def createExecutable(pipeline: LPipeline): LExecutable = {
    //TODO: Mapping over a property of a pipeline with a function that takes the pipeline feels weird.
    LExecutable(pipeline.tools.map(createJobs(_, pipeline)).collect { case Hit(job) => job }.flatten)
  }
  
  override def createExecutable(ast: AST): LExecutable = {
    val noJobs: Set[LJob] = Set.empty
    
    val jobs: Set[LJob] = ast match {
      case ToolNode(id, tool, deps) => {
        val jobsOption = for {
          //TODO: Don't convert to option, pass misses through and fail loudly
          job <- toolToJobShot(tool).asOpt
          newInputs = deps.map(_.producer).flatMap(createExecutable(_).jobs).toSet[LJob]
        } yield {
          Set[LJob](job.withInputs(newInputs))
        }
        
        jobsOption.getOrElse(noJobs)
      }
      case _ => noJobs //TODO: other AST nodes
    }
    
    LExecutable(jobs)
  }
}

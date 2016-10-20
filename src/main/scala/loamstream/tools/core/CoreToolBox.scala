package loamstream.tools.core

import java.nio.file.{Files, Path}

import scala.concurrent.{ ExecutionContext, Future }

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.Tool
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.model.jobs.Output
import loamstream.model.jobs.Output.PathOutput
import loamstream.model.jobs.commandline.CommandLineBuilderJob
import loamstream.tools._
import loamstream.tools.klusta.{KlustaKwikInputWriter, KlustaKwikKonfig, KlustaKwikLineCommand}
import loamstream.util._
import loamstream.model.jobs.JobState
import scala.util.Try
import scala.util.control.NonFatal

/**
 * LoamStream
 * Created by oliverr on 2/23/2016.
 */
object CoreToolBox extends LToolBox {

  trait CheckPreexistingFileJob extends LJob {
    def file: Path

    override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = Future {
      attempt {
        if (Files.exists(file)) { JobState.Succeeded }
        else { JobState.Failed }
      }
    }
    
    override val outputs: Set[Output] = Set(PathOutput(file))
  }

  final case class CheckPreexistingVcfFileJob(
      file: Path,
      inputs: Set[LJob] = Set.empty) extends CheckPreexistingFileJob {

    override def toString = s"CheckPreexistingVcfFileJob($file, ...)"

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
  }

  final case class CheckPreexistingPcaWeightsFileJob(
      file: Path,
      inputs: Set[LJob] = Set.empty) extends CheckPreexistingFileJob {

    override def toString = s"CheckPreexistingPcaWeightsFileJob($file, ...)"

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
  }

  final case class ExtractSampleIdsFromVcfFileJob(
      vcfFile: Path,
      samplesFile: Path,
      inputs: Set[LJob] = Set.empty) extends LJob {

    override val outputs: Set[Output] = Set(PathOutput(samplesFile))
    
    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = Futures.runBlocking {
      attempt {
        val samples = VcfParser(vcfFile).samples

        LoamFileUtils.printToFile(samplesFile.toFile) {
          p => samples.foreach(p.println) // scalastyle:ignore
        }

        JobState.Succeeded
      }
    }
  }

  final case class CalculatePcaProjectionsJob(vcfFile: Path,
                                              pcaWeightsFile: Path,
                                              klustaKwikKonfig: KlustaKwikKonfig,
                                              inputs: Set[LJob] = Set.empty) extends LJob {

    override def toString = s"CalculatePcaProjectionsJob($vcfFile, $pcaWeightsFile, ...)"

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override val outputs: Set[Output] = Set(PathOutput(klustaKwikKonfig.workDir))

    override protected def executeSelf(implicit context: ExecutionContext): Future[JobState] = Futures.runBlocking {
      attempt {
        val weights = PcaWeightsReader.read(pcaWeightsFile)
        val pcaProjecter = PcaProjecter(weights)
        val vcfParser = VcfParser(vcfFile)
        val samples = vcfParser.samples
        val genotypeToDouble: Genotype => Double = { genotype => VcfUtils.genotypeToAltCount(genotype).toDouble }
        val pcaProjections = pcaProjecter.project(samples, vcfParser.genotypeMapIter, genotypeToDouble)

        KlustaKwikInputWriter.writeFeatures(klustaKwikKonfig.inputFile, pcaProjections)

        JobState.Succeeded
      }
    }
  }

  private def attempt(f: => JobState): JobState = {
    try { f }
    catch {
      case NonFatal(e) => JobState.FailedWithException(e)
    }
  }
  
  def vcfFileJobShot(path: Path): Shot[CheckPreexistingVcfFileJob] = Hit(CheckPreexistingVcfFileJob(path))

  def pcaWeightsFileJobShot(path: Path): Shot[CheckPreexistingPcaWeightsFileJob] = {
    Hit(CheckPreexistingPcaWeightsFileJob(path))
  }

  def extractSamplesJobShot(vcfFile: Path, sampleFile: Path): Shot[ExtractSampleIdsFromVcfFileJob] = {
    Hit(ExtractSampleIdsFromVcfFileJob(vcfFile, sampleFile))
  }

  def calculatePcaProjectionsJobShot(
    vcfFile: Path,
    pcaWeightsFile: Path,
    klustaConfig: KlustaKwikKonfig): Shot[CalculatePcaProjectionsJob] = {

    Hit(CalculatePcaProjectionsJob(vcfFile, pcaWeightsFile, klustaConfig))
  }

  private def klustaKlwikCommandLine(klustaConfig: KlustaKwikKonfig): LineCommand.CommandLine = {
    import KlustaKwikLineCommand._

    klustaKwik(klustaConfig) + useDistributional(0)
  }

  def calculateClustersJobShot(klustaConfig: KlustaKwikKonfig): Shot[CommandLineBuilderJob] = Shot {
    CommandLineBuilderJob(
      klustaKlwikCommandLine(klustaConfig),
      klustaConfig.workDir,
      outputs = Set(PathOutput(klustaConfig.workDir)))
  }

  def toolToJobShot(tool: Tool): Shot[LJob] = tool match { //scalastyle:ignore
    case CoreTool.CheckPreExistingVcfFile(vcfFile)                 => vcfFileJobShot(vcfFile)

    case CoreTool.CheckPreExistingPcaWeightsFile(pcaWeightsFile)   => pcaWeightsFileJobShot(pcaWeightsFile)

    case CoreTool.ExtractSampleIdsFromVcfFile(vcfFile, sampleFile) => extractSamplesJobShot(vcfFile, sampleFile)

    case CoreTool.ProjectPcaNative(vcfFile, pcaWeightsFile, klustaKonfig) =>
      calculatePcaProjectionsJobShot(vcfFile, pcaWeightsFile, klustaKonfig)

    case CoreTool.ProjectPca(vcfFile, pcaWeightsFile, klustaKonfig) =>
      calculatePcaProjectionsJobShot(vcfFile, pcaWeightsFile, klustaKonfig)

    case CoreTool.KlustaKwikClustering(klustaConfig) => calculateClustersJobShot(klustaConfig)

    case CoreTool.ClusteringSamplesByFeatures(klustaConfig) => calculateClustersJobShot(klustaConfig)

    case _ => Miss(SnagMessage(s"Have not yet implemented tool $tool"))
  }

}

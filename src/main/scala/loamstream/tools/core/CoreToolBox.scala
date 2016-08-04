package loamstream.tools.core

import java.nio.file.{ Files, Path }

import scala.concurrent.{ ExecutionContext, Future }

import htsjdk.variant.variantcontext.Genotype
import loamstream.LEnv
import loamstream.model.Tool
import loamstream.model.jobs.{ LJob, LToolBox }
import loamstream.model.jobs.LJob.{ Result, SimpleFailure, SimpleSuccess }
import loamstream.model.jobs.commandline.CommandLineBuilderJob
import loamstream.tools.{ PcaProjecter, PcaWeightsReader, VcfParser }
import loamstream.tools.LineCommand
import loamstream.tools.VcfUtils
import loamstream.tools.klusta.{ KlustaKwikKonfig, KlustaKwikLineCommand }
import loamstream.tools.klusta.{ KlustaKwikLineCommand, KlustaKwikInputWriter }
import loamstream.util.{ Hit, Miss, Shot }
import loamstream.util.LoamFileUtils
import loamstream.util.SnagMessage

/**
 * LoamStream
 * Created by oliverr on 2/23/2016.
 */
object CoreToolBox extends LToolBox {

  final case class FileExists(path: Path) extends LJob.Success {
    override def successMessage: String = s"'$path' exists"
  }

  trait CheckPreexistingFileJob extends LJob {
    def file: Path

    override def execute(implicit context: ExecutionContext): Future[Result] = Future {
      Result.attempt {
        if (Files.exists(file)) { FileExists(file) }
        else { SimpleFailure(s"$file does not exist.") }
      }
    }
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

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      Result.attempt {
        val samples = VcfParser(vcfFile).samples

        LoamFileUtils.printToFile(samplesFile.toFile) {
          p => samples.foreach(p.println) // scalastyle:ignore
        }

        SimpleSuccess("Extracted sample ids.")
      }
    }
  }

  final case class CalculatePcaProjectionsJob(vcfFile: Path,
                                              pcaWeightsFile: Path,
                                              klustaKwikKonfig: KlustaKwikKonfig,
                                              inputs: Set[LJob] = Set.empty) extends LJob {

    override def toString = s"CalculatePcaProjectionsJob($vcfFile, $pcaWeightsFile, ...)"

    override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)

    override def execute(implicit context: ExecutionContext): Future[Result] = runBlocking {
      Result.attempt {
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

  private def pathShot(path: Path): Shot[Path] = {
    if (path.toFile.exists) { Hit(path) }
    else { Miss(s"Couldn't find '$path'") }
  }

  def sampleFileShot(path: Path): Shot[Path] = pathShot(path)

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
      Set.empty)
  }

  private def commandLineJobShot(tokens: Seq[String], workDir: Path): Shot[CommandLineBuilderJob] = {
    def commandLine(parts: Seq[String]): LineCommand.CommandLine = new LineCommand.CommandLine {
      override def tokens: Seq[String] = parts
      override def commandLine = tokens.mkString(LineCommand.tokenSep)
    }

    Shot(CommandLineBuilderJob(commandLine(tokens), workDir))
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

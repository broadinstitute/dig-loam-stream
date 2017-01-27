package loamstream.integration

import java.nio.file.{Path, Paths}

import loamstream.loam.LoamGraph.StoreLocation
import loamstream.loam.LoamProjectContext
import loamstream.model.jobs.LJob
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.util.BashScript
import loamstream.{LoamTestHelpers, TestHelpers}
import org.scalatest.FunSuite

/**
  * @author clint
  *         Jan 3, 2017
  */
final class KinshipStepHasRightStoresTest extends FunSuite with LoamTestHelpers {

  import TestHelpers.path

  test("Compiling the harmonize step produces the expected stores.") {
    //The loam files we want to compile
    val kinshipLoam = path("src/main/loam/qc/kinship.loam")
    val binariesLoam = path("src/main/loam/qc/binaries.loam")
    val configLoam = path("src/main/loam/qc/config.loam")
    val storeHelpersLoam = path("src/main/loam/qc/store_helpers.loam")

    val result = compile(configLoam, binariesLoam, storeHelpersLoam, kinshipLoam)

    assert(result.errors === Seq.empty)

    val context: LoamProjectContext = result.contextOpt.get

    def isRight(path: Path): Boolean = path.toString.endsWith("CAMP.kinship.pruned.bed")

    def isRightLocation(location: StoreLocation): Boolean = {
      location match {
        case StoreLocation.PathLocation(path) => isRight(path)
        case _ => false
      }
    }

    // Does an output store (sink) exist that's connected to a PathEdge representing
    // ./CAMP.kinship.pruned.bed?
    val outStores =
    context.graph.stores.filter(store => context.graph.storeLocations.get(store).exists(isRightLocation))
    assert(outStores.nonEmpty)
    assert(outStores.forall(!context.graph.inputStores.contains(_)))
    assert(outStores.forall(context.graph.storeProducers.contains))

    // Did we make the expected forest of job trees, and does the king command have its first
    // store param interpolated correctly?

    val (_, executable) = toExecutable(result)

    val Seq(rootJob) = executable.jobs.toSeq

    def firstDepOf(job: LJob): CommandLineJob = job.inputs.head.asInstanceOf[CommandLineJob]

    val kingJob = firstDepOf(firstDepOf(rootJob))

    val commandLine = kingJob.commandLineString

    val prunedBedPath = BashScript.escapeString(Paths.get(".", "CAMP.kinship.pruned.bed").toString)

    assert(commandLine.contains(s"king -b $prunedBedPath"))
  }
}

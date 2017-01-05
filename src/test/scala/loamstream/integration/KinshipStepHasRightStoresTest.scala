package loamstream.integration

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.LoamTestHelpers
import loamstream.loam.LoamProjectContext
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.loam.LoamToolBox
import loamstream.loam.LoamStore
import loamstream.compiler.LoamPredef
import loamstream.loam.ops.StoreType
import loamstream.loam.LoamGraph
import loamstream.loam.LoamGraph.StoreEdge.PathEdge
import java.nio.file.Path
import loamstream.model.jobs.commandline.CommandLineJob
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Jan 3, 2017
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
    
    def isRightEdge(edges: Set[LoamGraph.StoreEdge]): Boolean = {
      edges.collect { case p: PathEdge => p }.exists { case PathEdge(p) => isRight(p) }
    }
    
    // Does an output store (sink) exist that's connected to a PathEdge representing 
    // ./CAMP.kinship.pruned.bed?
    assert(context.graph.storeSinks.values.exists(isRightEdge)) 
    
    // Did we make the expected forest of job trees, and does the king command have its first
    // store param interpolated correctly?
    
    val (_, executable) = toExecutable(result)
    
    val Seq(rootJob) = executable.jobs.toSeq

    def firstDepOf(job: LJob): CommandLineJob = job.inputs.head.asInstanceOf[CommandLineJob]
    
    val kingJob = firstDepOf(firstDepOf(rootJob))
    
    val commandLine = kingJob.commandLineString
    
    assert(commandLine.contains("king -b ./CAMP.kinship.pruned.bed"))
  }
}

package loamstream.apps

import org.scalatest.FunSuite
import loamstream.cli.Conf
import loamstream.cli.Intent
import loamstream.cli.JobFilterIntent
import loamstream.model.execute.ByNameJobFilter
import loamstream.TestHelpers
import loamstream.drm.DrmSystem
import loamstream.compiler.LoamEngine

import scala.collection.immutable.ArraySeq

/**
 * @author clint
 * Jul 8, 2019
 */
final class RunAllOfTest extends FunSuite {
  test("--run allOf .pheno.tsv") {
    val conf = Conf(ArraySeq.unsafeWrapArray(
      ("--run allOf .pheno.tsv --conf src/test/resources/foo.conf " +
       "--dry-run --backend uger --loams src/examples/loam/cp.loam").split(' ')))
    
    val intentE = Intent.from(conf)
    
    val intent = intentE.getOrElse(sys.error(intentE.left.getOrElse(???))).asInstanceOf[Intent.DryRun]
    
    val runIfAllMatch = intent.jobFilterIntent.asInstanceOf[JobFilterIntent.RunIfAllMatch]
    
    val jobFilter = AppWiring.jobFilterForDryRun(intent, ???)
    
    assert(jobFilter.isInstanceOf[ByNameJobFilter.AllOf])
    
    val graph = TestHelpers.makeGraph(DrmSystem.Uger) { implicit ctx =>
      import loamstream.loam.LoamSyntax._
      
      val foo123 = cmd"foo --bar -n 123".tag("foo123")
      val bar = cmd"bar".tag("bar")
      val shouldMatch = cmd"la --la --la".tag("METSIM.EX_EUR.T2Dv1.pheno.tsv")
    }
    
    val allJobs = LoamEngine.toExecutable(graph).allJobs
    
    val foo123 = allJobs.find(_.name == "foo123").get
    val bar = allJobs.find(_.name == "bar").get
    val shouldMatch = allJobs.find(_.name == "METSIM.EX_EUR.T2Dv1.pheno.tsv").get
    
    assert(jobFilter.shouldRun(foo123) === false)
    assert(jobFilter.shouldRun(bar) === false)
    assert(jobFilter.shouldRun(shouldMatch) === true)
  }
}

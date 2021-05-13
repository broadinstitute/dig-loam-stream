package loamstream.model.execute

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.DataHandle.PathHandle
import loamstream.model.jobs.JobStatus
import loamstream.model.jobs.MockJob
import loamstream.util.Files

import scala.collection.compat._

/**
 * @author clint
 * Dec 15, 2020
 */
final class MissingOutputsJobFilterTest extends FunSuite {
  import MissingOutputsJobFilter.shouldRun
  import loamstream.TestHelpers.path
  
  test("shouldRun - no outputs") {
    val j0 = MockJob(JobStatus.Succeeded)
    
    assert(j0.outputs.isEmpty)
    
    assert(shouldRun(j0) === true)
  }
  
  test("shouldRun - missing outputs") {
    val outputs: Set[DataHandle] = Seq("foo/bar/baz/", "/blerg/nerg").map(path).map(PathHandle(_): DataHandle).to(Set)
    
    assert(outputs.forall(_.isMissing))
    
    //Multiple missing outputs
    {
      val j0 = MockJob(JobStatus.Succeeded, outputs = outputs)
    
      assert(j0.outputs === outputs)
      
      assert(shouldRun(j0) === true)
    }
    
    //One missing outputs
    {
      val justOne = outputs.take(1)
      
      val j0 = MockJob(JobStatus.Succeeded, outputs = justOne)
    
      assert(j0.outputs === justOne)
      
      assert(shouldRun(j0) === true)
    }
  }
  
  test("shouldRun - present outputs") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
     
      val paths = Seq("foo", "bar").map(workDir.resolve)
      
      for {
        p <- paths
      } {
        Files.writeTo(p)("sadf")
      }
      
      val outputs: Set[DataHandle] = paths.map(PathHandle(_): DataHandle).to(Set)
      
      assert(outputs.nonEmpty)
      
      assert(outputs.forall(_.isPresent))
      
      //Multiple missing outputs
      {
        val j1 = MockJob(JobStatus.Succeeded, name = "J1", outputs = outputs)
      
        assert(j1.outputs === outputs)
        
        assert(shouldRun(j1) === false)
      }
      
      //One missing output
      {
        val justOne = outputs.take(1)
        
        val j2 = MockJob(JobStatus.Succeeded, name = "J2", outputs = justOne)
      
        assert(j2.outputs === justOne)
        
        assert(shouldRun(j2) === false)
      }
    }
  }
  
  test("shouldRun - some present outputs, some missing outputs") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
     
      val paths = Seq("foo", "bar", "baz").map(workDir.resolve)
      
      for {
        p <- paths.take(2)
      } {
        Files.writeTo(p)("sadf")
      }
      
      val outputs: Seq[DataHandle] = paths.map(PathHandle(_))
      
      assert(outputs.nonEmpty)
      
      assert(outputs.take(2).forall(_.isPresent))
      assert(outputs.drop(2).forall(_.isMissing))
      
      //One missing, two present

      val j1 = MockJob(JobStatus.Succeeded, name = "J1", outputs = outputs.to(Set))
      
      assert(j1.outputs === outputs.to(Set))
        
      assert(shouldRun(j1) === true)
    }
  }
}

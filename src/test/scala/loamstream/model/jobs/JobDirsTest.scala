package loamstream.model.jobs

import org.scalatest.FunSuite
import scala.collection.Seq
import scala.annotation.tailrec
import loamstream.TestHelpers
import loamstream.model.execute.LocalSettings
import loamstream.util.Paths
import java.nio.file.Path

final class JobDirsTest extends FunSuite {
  import JobDirsTest.NamedJob
  import JobDirs.DirNode._
  
  test("Seq.splitInto") {
    import JobDirs.Implicits.SeqOps
    
    intercept[Exception] {
      Nil.splitInto(0)
    }
    assert(Nil.splitInto(1) === Nil)
    assert(Nil.splitInto(4) === Nil)
    
    intercept[Exception] {
      Seq(42).splitInto(0)
    }
    assert(Seq(42).splitInto(1) === Seq(Seq(42)))
    assert(Seq(42).splitInto(2) === Seq(Seq(42)))
    assert(Seq(42).splitInto(4) === Seq(Seq(42)))
    
    
    intercept[Exception] {
      Seq(1,2,3,4).splitInto(0)
    }
    assert(Seq(1,2,3,4).splitInto(1) === Seq(Seq(1, 2, 3, 4)))
    assert(Seq(1,2,3,4).splitInto(2) === Seq(Seq(1, 2), Seq(3, 4)))
    assert(Seq(1,2,3,4).splitInto(4) === Seq(Seq(1), Seq(2), Seq(3), Seq(4)))
    
    assert(Seq(1,2,3,4,5).splitInto(1) === Seq(Seq(1, 2, 3, 4, 5)))
    assert(Seq(1,2,3,4,5).splitInto(2) === Seq(Seq(1, 2, 3), Seq(4, 5)))
    assert(Seq(1,2,3,4,5).splitInto(3) === Seq(Seq(1, 2), Seq(3, 4), Seq(5)))
    assert(Seq(1,2,3,4,5).splitInto(4) === Seq(Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)))
    assert(Seq(1,2,3,4,5).splitInto(5) === Seq(Seq(1), Seq(2), Seq(3), Seq(4), Seq(5)))
  }
  
  test("toSimplePathName") {
    val jobFoo = NamedJob("foo")
    val jobFooBarBaz = NamedJob("foo/bar/baz")
    
    assert(Leaf(jobFoo).toSimplePathName === "foo")
    assert(Leaf(jobFooBarBaz).toSimplePathName === "foo_bar_baz")
    
    assert(Interior("x", Seq(Leaf(jobFoo))).toSimplePathName === "x")
    assert(Interior("x", Seq(Leaf(jobFoo), Leaf(jobFooBarBaz))).toSimplePathName === "x")
    
    assert(Interior("x/y/z", Seq(Leaf(jobFoo))).toSimplePathName === "x_y_z")
    assert(Interior("x/y/z", Seq(Leaf(jobFoo), Leaf(jobFooBarBaz))).toSimplePathName === "x_y_z")
  }
  
  test("Interior/Leaf guards") {
    val jobFoo = NamedJob("foo")
    val jobFooBarBaz = NamedJob("foo/bar/baz")
    
    intercept[Exception] {
      Interior("x", Seq.empty)
    }
    
    intercept[Exception] {
      Interior("", Seq(Leaf(jobFoo), Leaf(jobFooBarBaz)))
    }
    
    intercept[Exception] {
      Interior("", Seq.empty)
    }
    
    Interior("x", Seq(Leaf(jobFoo), Leaf(jobFooBarBaz)))
    
    intercept[Exception] {
      Leaf(NamedJob(""))
    }
    
    Leaf(NamedJob("asdf"))
  }
  
  test("Interior ids") {
    val jobFoo = NamedJob("foo")
    
    def ids(howMany: Int): Iterator[String] = {
      Iterator.continually(Interior(children = Seq(Leaf(jobFoo))).id).take(howMany) 
    }
    
    def allUnique[A](as: Iterator[A]): Boolean = {
      val counts: scala.collection.mutable.Map[A, Int] = scala.collection.mutable.Map.empty.withDefaultValue(0)
      
      as.foreach { a => 
        counts(a) = counts(a) + 1
      }
      
      counts.values.forall(_ == 1)
    }
    
    assert(ids(1000).forall(_.nonEmpty))
    
    assert(allUnique(ids(50000)))
  }
  
  test("pathsByJob - just a leaf") {
    import TestHelpers.path
    
    val jobFoo = NamedJob("foo")
    
    val leaf = Leaf(jobFoo)
    
    assert(leaf.pathsByJob(path("what/ever")) === Map(jobFoo -> path("what/ever/foo")))
  }
  
  test("pathsByJob - one level") {
    import TestHelpers.path
    
    val jobFoo = NamedJob("foo")
    val jobBar = NamedJob("bar")
    val jobBaz = NamedJob("baz")
    val jobBlerg = NamedJob("blerg")
    
    val interior = Interior("x", Seq(Leaf(jobFoo), Leaf(jobBar), Leaf(jobBaz), Leaf(jobBlerg))) 
    
    val expected = Map(
        jobFoo -> path("some/root/x/foo"),
        jobBar -> path("some/root/x/bar"),
        jobBaz -> path("some/root/x/baz"),
        jobBlerg -> path("some/root/x/blerg"))
    
    assert(interior.pathsByJob(path("some/root")) === expected)
  }
  
  test("pathsByJob - more levels") {
    import TestHelpers.path
    
    val jobFoo = NamedJob("foo")
    val jobBar = NamedJob("bar")
    
    val jobBaz = NamedJob("baz")
    val jobBlerg = NamedJob("blerg")
    
    val jobZerg = NamedJob("zerg")
    val jobNerg = NamedJob("nerg")

    val jobFlerg = NamedJob("flerg")
    val jobAsdf = NamedJob("asdf")
    
    val interior = Interior("0", 
                     Seq(Interior("1", Seq(
                           Interior("a", Seq(Leaf(jobFoo), Leaf(jobBar))),
                           Interior("b", Seq(Leaf(jobBaz), Leaf(jobBlerg)))
                         )),
                         Interior("2", Seq(
                           Interior("x", Seq(Leaf(jobZerg), Leaf(jobNerg))),
                           Interior("y", Seq(Leaf(jobFlerg), Leaf(jobAsdf)))
                             )))) 
    
    val expected = Map(
        jobFoo -> path("some/root/0/1/a/foo"),
        jobBar -> path("some/root/0/1/a/bar"),
        jobBaz -> path("some/root/0/1/b/baz"),
        jobBlerg -> path("some/root/0/1/b/blerg"),
        jobZerg -> path("some/root/0/2/x/zerg"),
        jobNerg -> path("some/root/0/2/x/nerg"),
        jobFlerg -> path("some/root/0/2/y/flerg"),
        jobAsdf -> path("some/root/0/2/y/asdf"))
    
    assert(interior.pathsByJob(path("some/root")) === expected)
  }
  
  test("makeDirsUnder - one level") {
    import TestHelpers.path
    
    val jobFoo = NamedJob("foo")
    val jobBar = NamedJob("bar")
    val jobBaz = NamedJob("baz")
    val jobBlerg = NamedJob("blerg")
    
    val interior = Interior("x", Seq(Leaf(jobFoo), Leaf(jobBar), Leaf(jobBaz), Leaf(jobBlerg))) 
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      import Paths.Implicits._
      import java.nio.file.Files.exists
      import java.nio.file.Files.isDirectory
      
      val expectedDirs: Seq[Path] = Array(
        workDir / "x" / "foo",
        workDir / "x" / "bar",
        workDir / "x" / "baz",
        workDir / "x" / "blerg")
        
      assert(exists(expectedDirs(0)) === false)
      assert(exists(expectedDirs(1)) === false)
      assert(exists(expectedDirs(2)) === false)
      assert(exists(expectedDirs(3)) === false)
      
      assert(interior.makeDirsUnder(workDir))
      
      assert(exists(expectedDirs(0)) === true)
      assert(exists(expectedDirs(1)) === true)
      assert(exists(expectedDirs(2)) === true)
      assert(exists(expectedDirs(3)) === true)
      
      assert(isDirectory(expectedDirs(0)) === true)
      assert(isDirectory(expectedDirs(1)) === true)
      assert(isDirectory(expectedDirs(2)) === true)
      assert(isDirectory(expectedDirs(3)) === true)
    }
  }
  
  test("makeDirsUnder - more levels") {
    import TestHelpers.path
    
    val jobFoo = NamedJob("foo")
    val jobBar = NamedJob("bar")
    
    val jobBaz = NamedJob("baz")
    val jobBlerg = NamedJob("blerg")
    
    val jobZerg = NamedJob("zerg")
    val jobNerg = NamedJob("nerg")

    val jobFlerg = NamedJob("flerg")
    val jobAsdf = NamedJob("asdf")
    
    val interior = Interior("0", 
                     Seq(Interior("1", Seq(
                           Interior("a", Seq(Leaf(jobFoo), Leaf(jobBar))),
                           Interior("b", Seq(Leaf(jobBaz), Leaf(jobBlerg)))
                         )),
                         Interior("2", Seq(
                           Interior("x", Seq(Leaf(jobZerg), Leaf(jobNerg))),
                           Interior("y", Seq(Leaf(jobFlerg), Leaf(jobAsdf)))
                             )))) 
    
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      import Paths.Implicits._
      import java.nio.file.Files.exists
      import java.nio.file.Files.isDirectory
                             
      val expectedDirs: Seq[Path] = Array(
        workDir / "0" / "1" / "a" / "foo",
        workDir / "0" / "1" / "a" / "bar",
        workDir / "0" / "1" / "b" / "baz",
        workDir / "0" / "1" / "b" / "blerg",
        workDir / "0" / "2" / "x" / "zerg",
        workDir / "0" / "2" / "x" / "nerg",
        workDir / "0" / "2" / "y" / "flerg",
        workDir / "0" / "2" / "y" / "asdf")
    
      assert(exists(expectedDirs(0)) === false)
      assert(exists(expectedDirs(1)) === false)
      assert(exists(expectedDirs(2)) === false)
      assert(exists(expectedDirs(3)) === false)
      assert(exists(expectedDirs(4)) === false)
      assert(exists(expectedDirs(5)) === false)
      assert(exists(expectedDirs(6)) === false)
      assert(exists(expectedDirs(7)) === false)
      
      assert(interior.makeDirsUnder(workDir))
      
      assert(exists(expectedDirs(0)) === true)
      assert(exists(expectedDirs(1)) === true)
      assert(exists(expectedDirs(2)) === true)
      assert(exists(expectedDirs(3)) === true)
      assert(exists(expectedDirs(4)) === true)
      assert(exists(expectedDirs(5)) === true)
      assert(exists(expectedDirs(6)) === true)
      assert(exists(expectedDirs(7)) === true)
      
      assert(isDirectory(expectedDirs(0)) === true)
      assert(isDirectory(expectedDirs(1)) === true)
      assert(isDirectory(expectedDirs(2)) === true)
      assert(isDirectory(expectedDirs(3)) === true)
      assert(isDirectory(expectedDirs(4)) === true)
      assert(isDirectory(expectedDirs(5)) === true)
      assert(isDirectory(expectedDirs(6)) === true)
      assert(isDirectory(expectedDirs(7)) === true)
    }
  }
  
  test("allocate - guards") {
    import JobDirs.allocate
    
    val job = NamedJob("foo")
    
    intercept[Exception] {
      allocate(Nil, 0)
    }
    
    intercept[Exception] {
      allocate(Nil, 1)
    }
    
    intercept[Exception] {
      allocate(Nil, 42)
    }
    
    intercept[Exception] {
      allocate(Seq(job), 0)
    }
    
    intercept[Exception] {
      allocate(Seq(job), 1)
    }
  }
  
  test("allocate - fewer jobs than branching factor") {
    val job0 = NamedJob("foo")
    val job1 = NamedJob("bar")
    
    val ids = Iterator.iterate(0)(_ + 1).map(_.toString)
    
    val nextId = () => ids.next()
    
    val node = JobDirs.allocate(Seq(job0, job1), 4, nextId)
    
    assert(node === Interior("0", Seq(Leaf(job0), Leaf(job1))))
  }
  
  test("allocate - more jobs than branching factor") {
    import TestHelpers.path
    
    val jobFoo = NamedJob("foo")
    val jobBar = NamedJob("bar")
    
    val jobBaz = NamedJob("baz")
    val jobBlerg = NamedJob("blerg")
    
    val jobZerg = NamedJob("zerg")
    val jobNerg = NamedJob("nerg")

    val jobFlerg = NamedJob("flerg")
    val jobAsdf = NamedJob("asdf")
    
    val ids = Iterator.iterate(0)(_ + 1).map(_.toString)
    
    val nextId = () => ids.next()
    
    val expected = Interior("0", 
                     Seq(Interior("1", Seq(
                           Interior("2", Seq(Leaf(jobFoo), Leaf(jobBar))),
                           Interior("3", Seq(Leaf(jobBaz), Leaf(jobBlerg)))
                         )),
                         Interior("4", Seq(
                           Interior("5", Seq(Leaf(jobZerg), Leaf(jobNerg))),
                           Interior("6", Seq(Leaf(jobFlerg), Leaf(jobAsdf)))
                             ))))
                             
    val jobs = Seq(jobFoo, jobBar, jobBaz, jobBlerg, jobZerg, jobNerg, jobFlerg, jobAsdf)
                             
    val node = JobDirs.allocate(jobs, 2, nextId)
    
    assert(node === expected)
  }
  
  test("allocate - lots of jobs") {
    val jobNames = Iterator.iterate(0)(_ + 1).map(_.toString)
    
    val jobs = jobNames.take(20000).map(NamedJob(_))
    
    val branchingFactor = 500
    
    val node = JobDirs.allocate(jobs.toSeq, branchingFactor)
    
    assert(node.isInstanceOf[Interior])
    
    def isValid(n: JobDirs.DirNode): Boolean = n match {
      case _: Leaf => true
      case Interior(_, children) => children.size <= branchingFactor && children.forall(isValid)
    }
    
    assert(isValid(node))
  }
}

object JobDirsTest {
  private final case class NamedJob(override val name: String) extends 
      MockJob(name, dependencies = Set.empty, inputs = Set.empty, outputs = Set.empty, delay = 0) {

    override def toReturn: RunData = {
      RunData(
        job = this,
        settings = LocalSettings,
        jobStatus = JobStatus.Succeeded,
        jobResult = Some(JobResult.Success),
        terminationReasonOpt = None)
    }
  }
}

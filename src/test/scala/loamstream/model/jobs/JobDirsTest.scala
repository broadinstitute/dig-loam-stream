package loamstream.model.jobs

import java.nio.file.Path

import scala.collection.Seq

import org.scalatest.FunSuite

import loamstream.TestHelpers
import loamstream.model.execute.LocalSettings
import loamstream.util.Paths
import loamstream.model.jobs.DirTree.DirNode
import org.scalactic.Equality

/**
 * @author clint
 * Sep 26, 2019
 */
final class JobDirsTest extends FunSuite {
  import DirTree.DirNode._
  import JobDirsTest.NamedJob
  import JobDirsTest.DirNodeOps
  
  private implicit def dirNodeEquality[A]: Equality[DirNode[A]] = new Equality[DirNode[A]] {
    override def areEqual(lhs: DirNode[A], a: Any): Boolean = a match {
      case rhs: DirNode[_] => lhs.equalsWithoutId(rhs)
      case _ => false
    }
  }
  
  test("findHeight") {
    import DirTree.findHeight
    
    assert(findHeight(1, 0) === 0)
    assert(findHeight(2, 0) === 0)
    
    assert(findHeight(2, 1) === 0)
    assert(findHeight(4, 1) === 0)
    assert(findHeight(8, 1) === 0)
    
    assert(findHeight(2, 2) === 1)
    assert(findHeight(4, 2) === 1)
    assert(findHeight(8, 2) === 1)
    
    assert(findHeight(2, 2) === 1)
    assert(findHeight(4, 4) === 1)
    assert(findHeight(8, 8) === 1)
    
    assert(findHeight(2, 4) === 2)
    assert(findHeight(4, 8) === 2)
    assert(findHeight(8, 16) === 2)
    
    assert(findHeight(10, 11) === 2)
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
      Interior("x", Seq.empty[DirNode[LJob]])
    }
    
    intercept[Exception] {
      Interior("", Seq(Leaf(jobFoo), Leaf(jobFooBarBaz)))
    }
    
    intercept[Exception] {
      Interior("", Seq.empty[DirNode[LJob]])
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
  
  test("pathsByValue - just a leaf") {
    import TestHelpers.path
    
    val jobFoo = NamedJob("foo")
    
    val leaf = Leaf(jobFoo)
    
    assert(leaf.pathsByValue(path("what/ever")) === Map(jobFoo -> path("what/ever/foo")))
  }
  
  test("pathsByValue - one level") {
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
    
    assert(interior.pathsByValue(path("some/root")) === expected)
  }
  
  test("pathsByValue - more levels") {
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
    
    assert(interior.pathsByValue(path("some/root")) === expected)
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
    import DirTree.allocate
    
    val job = NamedJob("foo")
    
    val empty: Seq[LJob] = Nil
    
    intercept[Exception] {
      allocate(empty, 0)
    }
    
    intercept[Exception] {
      allocate(empty, 1)
    }
    
    intercept[Exception] {
      allocate(empty, 42)
    }
    
    intercept[Exception] {
      allocate(Seq(job), 0)
    }
  }
  
  test("allocate - fewer jobs than branching factor") {
    val job0 = NamedJob("foo")
    val job1 = NamedJob("bar")
    
    val ids = Iterator.iterate(0)(_ + 1).map(_.toString)
    
    val nextId = () => ids.next()
    
    val node = DirTree.allocate(Seq(job0, job1), 4, nextId)
    
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
                             
    val node = DirTree.allocate(jobs, 2, nextId)
    
    assert(node === expected)
  }
  
  test("allocate - lots of jobs") {
    val jobNames = Iterator.iterate(0)(_ + 1).map(_.toString)
    
    val jobs: Iterator[LJob] = jobNames.take(20000).map(NamedJob(_))
    
    val branchingFactor = 500
    
    val node = DirTree.allocate(jobs.toSeq, branchingFactor)
    
    assert(node.isInstanceOf[Interior[_]])
    
    def isValid(n: DirTree.DirNode[LJob]): Boolean = n match {
      case _: Leaf[_] => true
      case Interior(_, children) => children.size <= branchingFactor && children.forall(isValid)
    }
    
    assert(isValid(node))
  }
  
  test("allocate - lots of jobs 2") {
    val jobNames = Iterator.iterate(0)(_ + 1).map(_.toString)
    
    val jobs = jobNames.take(11).map(NamedJob(_))
    
    val branchingFactor = 10
    
    val node: DirNode[LJob] = DirTree.allocate(jobs.toSeq, branchingFactor)
    
    val root = node.asInstanceOf[Interior[_]]
    
    assert(root.children.size == 2)
    
    def isValid(n: DirTree.DirNode[LJob]): Boolean = n match {
      case _: Leaf[_] => true
      case Interior(_, children) => children.size <= branchingFactor && children.forall(isValid)
    }
    
    assert(isValid(node))
    
    val leftBranch = root.children.head.asInstanceOf[DirNode.Interior[LJob]]
    val rightBranch = root.children.drop(1).head.asInstanceOf[DirNode.Interior[LJob]]
    
    val lhsLeaves = leftBranch.children.map(_.asInstanceOf[DirNode.Leaf[LJob]])
    val rhsLeaves = rightBranch.children.map(_.asInstanceOf[DirNode.Leaf[LJob]])
    
    assert(lhsLeaves.map(_.value.name).toSet === (0 to 9).map(_.toString).toSet)
    assert(rhsLeaves.map(_.value.name).toSet === Set("10"))
  }
}

object JobDirsTest {
  private final case class NamedJob(override val name: String) extends 
      MockJob(
          name, 
          dependencies = Set.empty, 
          successorsFn = () => Set.empty, 
          inputs = Set.empty, 
          outputs = Set.empty, 
          delay = 0) {

    override def toReturn: RunData = {
      RunData(
        job = this,
        settings = LocalSettings,
        jobStatus = JobStatus.Succeeded,
        jobResult = Some(JobResult.Success),
        terminationReasonOpt = None)
    }
  }
  
  private final implicit class DirNodeOps(val dirNode: DirNode[_]) extends AnyVal {
    def equalsWithoutId(other: DirNode[_]): Boolean = (dirNode, other) match {
      case (DirNode.Interior(_, lhsChildren), DirNode.Interior(_, rhsChildren)) => {
        lhsChildren.iterator.zip(rhsChildren.iterator).forall { case (l, r) => l.equalsWithoutId(r) }
      }
      case (lhs: DirNode.Leaf[_], rhs: DirNode.Leaf[_]) => lhs == rhs
      case _ => false
    }
  }
}

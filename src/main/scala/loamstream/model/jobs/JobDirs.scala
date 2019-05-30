package loamstream.model.jobs

import scala.collection.Seq
import loamstream.util.{ Paths => LPaths }
import loamstream.util.{ Files => LFiles }
import loamstream.util.Sequence
import java.nio.file.Path
import java.nio.file.Paths.{ get => path }
import java.nio.file.Files
import scala.util.Try


object JobDirs {

  def allocate(jobs: Iterable[LJob], branchingFactor: Int, nextId: () => String = () => DirNode.nextId()): DirNode = {
    require(branchingFactor > 1)
    require(jobs.nonEmpty)
    
    if(jobs.size <= branchingFactor) {
      DirNode.Interior(id = nextId(), children = jobs.map(DirNode.Leaf(_)))
    } else {
      val numChunks = branchingFactor
      
      import Implicits.SeqOps      
      
      DirNode.Interior(
          id = nextId(), 
          children = jobs.toSeq.splitInto(numChunks).map(chunk => allocate(chunk, branchingFactor, nextId)))
    }
  }
  
  sealed trait DirNode {
    def toSimplePathName: String = LPaths.mungePathRelatedChars {
      this match {
        case l: DirNode.Leaf => l.job.name
        case i: DirNode.Interior => i.id
      }
    }
    
    def pathsByJob(jobDataDir: Path): Map[LJob, Path] = {
      import LPaths.Implicits._

      def doPathsByJob(root: Path)(n: DirNode): Iterable[(LJob, Path)] = n match {
        case l @ DirNode.Leaf(job) => Seq(job -> (root / l.toSimplePathName).normalize)
        case i @ DirNode.Interior(id, children) => {
          val nodeRoot = root / i.toSimplePathName
          
          children.flatMap(doPathsByJob(nodeRoot))
        }
      }
      
      doPathsByJob(jobDataDir)(this).toMap
    }
    
    def makeDirsUnder(root: Path): Boolean = {
      import LPaths.Implicits._
      import LFiles.createDirsIfNecessary
      
      val pathsToCreate = pathsByJob(root).values.iterator
      
      val attempts = pathsToCreate.map(p => Try(createDirsIfNecessary(p)))
      
      attempts.forall(_.isSuccess)
    }
  }
  
  object DirNode {
    private[this] val lock = new AnyRef
    
    private[this] val ids: Iterator[String] = {
      val alphabet = "abcdefghijklmnopqrstuvwxyz".toUpperCase

      def is = Iterator.iterate(0)(_ + 1)
      def alphabetIter = alphabet.iterator.map(_.toString)
      
      def lettersPlus(i: Int): Iterator[String] = alphabetIter.map(l => s"${l}${i}")
      
      //A1..Z1 A2..Z2 ... AN..ZN
      
      is.flatMap(lettersPlus) ++ ids 
    }
    
    private[JobDirs] def nextId(): String = lock.synchronized(ids.next())
    
    final case class Interior(id: String = nextId(), children: Iterable[DirNode]) extends DirNode {
      require(id.nonEmpty)
      require(children.nonEmpty)
    }
    
    final case class Leaf(job: LJob) extends DirNode {
      require(job.name.nonEmpty)
    }
  }
  
  private[jobs] object Implicits {
    final implicit class SeqOps[A](val as: Seq[A]) extends AnyVal {
      def splitInto(numChunks: Int): Seq[Seq[A]] = {
        require(numChunks > 0)
        
        if(as.isEmpty) { Nil }
        else if(as.size < numChunks) { Seq(as) }
        else {
          val chunkSize = scala.math.round(as.size.toFloat / numChunks)
          
          as.grouped(chunkSize).toIndexedSeq
        }
      }
    }
  }
}

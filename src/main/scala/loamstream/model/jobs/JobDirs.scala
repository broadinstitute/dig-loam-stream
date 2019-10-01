package loamstream.model.jobs

import java.nio.file.Path

import scala.util.Try

import loamstream.util.{ Paths => LPaths }

/**
 * @author clint
 * Jul 2019
 */
object JobDirs {

  def allocate(jobs: Iterable[LJob], branchingFactor: Int, nextId: () => String = () => DirNode.nextId()): DirNode = {
    require(jobs.nonEmpty)
    
    val numJobs = jobs.size
    
    val height = findHeight(branchingFactor, numJobs)
    
    require(height >= 0)
    
    def interior(children: Iterable[DirNode]): Option[DirNode.Interior] = {
      if(children.nonEmpty) { Some(DirNode.Interior(id = nextId(), children = children)) }
      else { None }
    }
    
    val jobChunks = jobs.grouped(branchingFactor)
    
    def makeInteriorNodesAtHeight(h: Int): Option[DirNode.Interior] = h match {
      case 0 if jobChunks.hasNext => interior(children = jobChunks.next().map(DirNode.Leaf))
      case n if jobChunks.hasNext => {
        interior(children = (1 to branchingFactor).flatMap(_ => makeInteriorNodesAtHeight(n - 1)))
      }
      case _ => None
    }
    
    def empty = DirNode.Interior(children = Nil)

    (height, jobs.headOption) match {
      case (0, Some(job)) => DirNode.Leaf(job)
      case (_, None) => empty
      case _ => makeInteriorNodesAtHeight(height - 1).getOrElse(empty)
    }
  }
  
  private[jobs] def findHeight(branchingFactor: Int, desiredLeaves: Int): Int = {
    if(desiredLeaves == 0) { 0 }
    else {
      import scala.math.{ ceil, log }
      
      //NB: log(desiredLeaves) / log(branchingFactor) == log base-branchingFactor of desiredLeaves
      ceil(log(desiredLeaves) / log(branchingFactor)).toInt
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
      import loamstream.util.Paths.Implicits._

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
      import loamstream.util.Files.createDirsIfNecessary
      
      val pathsToCreate = pathsByJob(root).values.iterator
      
      val attempts = pathsToCreate.map(p => Try(createDirsIfNecessary(p)))
      
      attempts.forall(_.isSuccess)
    }
  }
  
  object DirNode {
    private[this] val lock = new AnyRef
    
    private[this] val ids: Iterator[String] = {
      val alphabet = 'A' to 'Z'

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
      
      override def equals(other: Any): Boolean = other match {
        case that: Interior => this.children == that.children
        case _ => false
      }
      
      override def hashCode: Int = children.hashCode
    }
    
    final case class Leaf(job: LJob) extends DirNode {
      require(job.name.nonEmpty)
    }
  }
}

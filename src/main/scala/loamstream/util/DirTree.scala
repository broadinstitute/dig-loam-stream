package loamstream.util

import java.nio.file.Path

import scala.util.Try

import loamstream.model.jobs.LJob
import loamstream.util.{ Paths => LPaths }

/**
 * @author clint
 * Jul 2019
 */
object DirTree {

  def allocate[A : DirNode.CanBeASimplePath](
      as: Iterable[A], 
      branchingFactor: Int, 
      nextId: () => String = () => DirNode.nextId()): DirNode[A] = {
    
    require(as.nonEmpty)
    
    val numValues = as.size
    
    val height = findHeight(branchingFactor, numValues)
    
    require(height >= 0)
    
    def interior(children: Iterable[DirNode[A]]): Option[DirNode.Interior[A]] = {
      if(children.nonEmpty) { Some(DirNode.Interior(id = nextId(), children = children)) }
      else { None }
    }
    
    val chunks = as.grouped(branchingFactor)
    
    def makeInteriorNodesAtHeight(h: Int): Option[DirNode.Interior[A]] = h match {
      case 0 if chunks.hasNext => interior(children = chunks.next().map(DirNode.Leaf(_)))
      case n if chunks.hasNext => {
        interior(children = (1 to branchingFactor).flatMap(_ => makeInteriorNodesAtHeight(n - 1)))
      }
      case _ => None
    }
    
    def empty = DirNode.Interior[A](children = Nil)

    (height, as.headOption) match {
      case (0, Some(value)) => DirNode.Leaf(value)
      case (_, None) => empty
      case _ => makeInteriorNodesAtHeight(height - 1).getOrElse(empty)
    }
  }
  
  private[util] def findHeight(branchingFactor: Int, desiredLeaves: Int): Int = {
    if(desiredLeaves == 0) { 0 }
    else {
      import scala.math.{ ceil, log }
      
      //NB: log(desiredLeaves) / log(branchingFactor) == log base-branchingFactor of desiredLeaves
      ceil(log(desiredLeaves) / log(branchingFactor)).toInt
    }
  }
  
  sealed trait DirNode[A] {
    def toSimplePathName: String
    
    def pathsByValue(rootDir: Path): Map[A, Path] = {
      import loamstream.util.Paths.Implicits._

      def doPathsByValue(root: Path)(n: DirNode[A]): Iterable[(A, Path)] = n match {
        case l @ DirNode.Leaf(value) => Seq(value -> (root / l.toSimplePathName).normalize)
        case i @ DirNode.Interior(id, children) => {
          val nodeRoot = root / i.toSimplePathName
          
          children.flatMap(doPathsByValue(nodeRoot))
        }
      }
      
      doPathsByValue(rootDir)(this).toMap
    }
    
    def makeDirsUnder(root: Path): Boolean = {
      import loamstream.util.Files.createDirsIfNecessary
      
      val pathsToCreate = pathsByValue(root).values.iterator
      
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
    
    private[DirTree] def nextId(): String = lock.synchronized(ids.next())

    sealed trait CanBeASimplePath[A] {
      def toSimplePathName(a: A): String
    }
    
    object CanBeASimplePath {
      implicit def jobsCanBeSimplePaths[J <: LJob]: CanBeASimplePath[J] = new CanBeASimplePath[J] {
        override def toSimplePathName(j: J): String = LPaths.mungePathRelatedChars(j.name)
      }
    }
    
    final case class Interior[A: CanBeASimplePath](id: String = nextId(), children: Iterable[DirNode[A]]) extends DirNode[A] {
      require(id.nonEmpty)
      require(children.nonEmpty)
      
      override def toSimplePathName: String = LPaths.mungePathRelatedChars(id)
    }
    
    final case class Leaf[A](value: A)(implicit ev: CanBeASimplePath[A]) extends DirNode[A] {
      require(toSimplePathName.nonEmpty)
      
      override def toSimplePathName: String = ev.toSimplePathName(value)
    }
  }
}

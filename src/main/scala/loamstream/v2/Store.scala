package loamstream.v2

import java.nio.file.Path
import loamstream.model.LId
import java.nio.file.Paths
import java.io.File
import loamstream.util.Functions
import scala.annotation.tailrec

sealed trait Store extends LId.HasId {
  def render: String
  
  def derivedFrom: Option[Store]
  
  final def ancestors: Seq[Store] = {
    @tailrec
    def loop(current: Store, acc: Seq[Store]): Seq[Store] = current.derivedFrom match {
      case None => acc
      case Some(parent) => loop(parent, acc :+ parent)
    }
    
    loop(this, Vector.empty)
  }
}

object Store {
  private def newTempFile: Path = File.createTempFile("loamstream", "store").toPath
  
  import Functions.memoize
  
  final case class ValueStore[+A] private (
      id: LId, 
      value: () => A, 
      derivedFrom: Option[Store])(implicit context: Context) extends Store {
    
    override def render: String = value().toString
    
    def map[B](f: A => B): ValueStore[B] = ValueStore(f(value()), this)
    
    def flatMap[B](f: A => ValueStore[B]): ValueStore[B] = ValueStore(f(value()).value(), this)
  }
  
  object ValueStore {
    def apply[A](value: => A)(implicit context: Context): ValueStore[A] = create(value, None)
    
    def apply[A](value: => A, derivedFrom: Store)(implicit context: Context): ValueStore[A] = {
      create(value, Option(derivedFrom))
    }
    
    private def create[A](value: => A, derivedFrom: Option[Store])(implicit context: Context): ValueStore[A] = {
      context.register(new ValueStore(LId.newAnonId, memoize(() => value), derivedFrom))
    }
  }
  
  final case class FileStore private (id: LId, derivedFrom: Option[Store], path: Path) extends Store {
    override def render: String = path.toAbsolutePath.toString
  }
  
  object FileStore {
    def apply(
        id: LId = LId.newAnonId, 
        path: Path = newTempFile,
        derivedFrom: Option[Store] = None)(implicit context: Context): FileStore = {
      
      context.register(new FileStore(id, derivedFrom, path))
    }
  }
}

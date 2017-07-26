package loamstream.v2

import java.nio.file.Path
import loamstream.model.LId
import java.nio.file.Paths
import java.io.File
import loamstream.util.Functions

sealed trait Store extends LId.Owner {
  def render: String
}

object Store {
  private def newTempFile: Path = File.createTempFile("loamstream", "store").toPath
  
  import Functions.memoize
  
  final case class ValueStore[+A] private (id: LId, value: () => A) extends Store {
    override def render: String = value.toString
    
    def map[B](f: A => B): ValueStore[B] = ValueStore(f(value()))
    
    def flatMap[B](f: A => ValueStore[B]): ValueStore[B] = ValueStore(f(value()).value())
  }
  
  object ValueStore {
    def apply[A](value: => A): ValueStore[A] = {
      new ValueStore(LId.newAnonId, memoize(() => value))
    }
  }
  
  final case class FileStore private (id: LId, path: Path = newTempFile) extends Store {
    override def render: String = path.toAbsolutePath.toString
    
    def at(newPath: Path): FileStore = copy(path = newPath)
  }
}

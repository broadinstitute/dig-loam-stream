package loamstream.v2

import scala.language.higherKinds

import loamstream.v2.Store.FileStore
import loamstream.v2.Store.ValueStore
import loamstream.model.LId
import java.nio.file.Path
import java.nio.file.Paths
import scala.collection.generic.CanBuildFrom
import scala.collection.immutable.Range

object V2Predef {
  implicit final class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Any*)(implicit context: Context): Command = {
      
      def stringToken(arg: Any): Token.StringToken = Token.StringToken(arg.toString)
      
      def toToken(arg: Any): Token = arg match {
        case store: Store => Token.StoreToken(store.id)
        case arg => stringToken(arg.toString)
      }
      
      val firstPart +: stringParts = stringContext.parts

      val firstToken: Token = Token.StringToken(firstPart)

      val tokens: Seq[Token] = firstToken +: {
        stringParts.zip(args).flatMap { case (stringPart, arg) =>
          Seq(toToken(arg), stringToken(stringPart))
        }
      }
      
      val storeDeps = tokens.collect { case Token.StoreToken(id) => id }.toSet
      
      val command = Command(tokens: _*)(storeDeps)
      
      context.register(command)
      
      command
    }
  }
  
  def path(s: String): Path = Paths.get(s)
  
  private[this] def register(s: Store)(implicit context: Context): Store = {
    context.register(s)
    
    s
  }
  
  def store(implicit context: Context): Store = register(FileStore(LId.newAnonId))
  
  def store(fileName: String)(implicit context: Context): Store = store(path(fileName))
  
  def store(p: Path)(implicit context: Context): Store = register(FileStore(LId.newAnonId, p))
  
  def value[A](v: => A): ValueStore[A] = ValueStore(v)
  
  def lift[A](f: Path => A): Store => A = {
    case FileStore(_, path) => f(path)
    case _ => ???
  }
  
  def loop[A, B, C[X] <: TraversableOnce[X]]
      (as: ValueStore[C[A]])
      (body: A => B)
      (implicit cbf: CanBuildFrom[C[A], B, C[B]], context: Context): Task[C[B]] = {
    
    Task {
      val builder = cbf()
      
      as.value().foreach { a => 
        builder += body(a)
      }
      
      builder.result()
    }
  }
  
  def loop[A](times: ValueStore[Int])(body: Int => A)(implicit context: Context): Task[Seq[A]] = {
    val range: ValueStore[IndexedSeq[Int]] = times.map(t => 0 until t)
    
    loop(range)(body)
  }
}

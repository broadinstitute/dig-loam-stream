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
        //TODO: Address unchecked warning 
        case stores: Traversable[Store] => Token.MultiStoreToken(stores.map(_.id))
        case arg => stringToken(arg.toString)
      }
      
      val firstPart +: stringParts = stringContext.parts

      val firstToken: Token = Token.StringToken(firstPart)

      val tokens: Seq[Token] = firstToken +: {
        stringParts.zip(args).flatMap { case (stringPart, arg) =>
          Seq(toToken(arg), stringToken(stringPart))
        }
      }
      
      val storeDeps = tokens.flatMap { 
        case Token.MultiStoreToken(ids) => ids
        case Token.StoreToken(id) => Seq(id) 
        case _ => Nil
      }.toSet
      
      val command = Command(tokens: _*)(storeDeps)
      
      context.register(command)
      
      command
    }
  }
  
  def path(s: String): Path = Paths.get(s)
  
  def store(implicit context: Context): Store = context.register(FileStore(LId.newAnonId))
  
  def store(fileName: String)(implicit context: Context): Store = store(path(fileName))
  
  def store(p: Path)(implicit context: Context): Store = context.register(FileStore(LId.newAnonId, p))
  
  def value[A](v: => A)(implicit context: Context): ValueStore[A] = context.register(ValueStore(v))
  
  //TODO: ???
  def lift[A](f: Path => A): Store => A = {
    case FileStore(_, _, path) => f(path)
    case _ => ???
  }
  
  def loop[A, B, C[X] <: Traversable[X]]
      (as: ValueStore[C[A]])
      (body: A => B)
      (implicit cbf: CanBuildFrom[Traversable[A], B, C[B]], context: Context): Task[C[B]] = {
    
    Task {
      as.value().map(body)(cbf)
      
      /*val builder = cbf()
      
      as.value().foreach { a => 
        builder += body(a)
      }
      
      builder.result()*/
    }
  }
  
  def loop[A](times: ValueStore[Int])(body: Int => A)(implicit context: Context): Task[Seq[A]] = {
    val range: ValueStore[IndexedSeq[Int]] = times.map(t => 0 until t)
    
    loop(range)(body)
  }
}

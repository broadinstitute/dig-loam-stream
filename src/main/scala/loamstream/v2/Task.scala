package loamstream.v2

import loamstream.util.Functions
import loamstream.model.LId
import scala.util.control.NonFatal
import scala.collection.generic.CanBuildFrom

final case class Task[+A] private (body: () => A)(implicit override val context: Context) extends Tool {
  def perform(): ToolState = {
    try { 
      body()

      ToolState.Finished
    } catch {
      case NonFatal(e) => ToolState.Failed
    }
  }
  
  override def referencedStores: Set[LId] = Set.empty 
  
  def map[B](f: A => B): Task[B] = Task(f(body()))
  
  def flatMap[B](f: A => Task[B]): Task[B] = Task(f(body()).body())
}

object Task {
  def apply[A](body: => A)(implicit context: Context, discriminator: Int = 42): Task[A] = {
    val newTask = new Task(Functions.memoize(() => body))
    
    context.register(newTask)
    
    newTask
  }
}

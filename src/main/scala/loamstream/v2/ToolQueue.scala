package loamstream.v2

import loamstream.util.ValueBox
import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.Observable

final class ToolQueue {
  /*private val toBeRun: ValueBox[Set[Tool]] = ValueBox(Set.empty) 
  
  def nonEmpty: Boolean = toBeRun.get { eligible =>
    eligible.nonEmpty || eligible.exists(canRun)
  } 
  
  def isEmpty: Boolean = !nonEmpty
  
  def next(): Tool = toBeRun.get { eligible =>
    require(!isEmpty)
    
    val (result, remaining) = {
      val (canRunNow, cannotRunNow) = eligible.iterator.partition(canRun)
      
      (canRunNow.next(), canRunNow ++ cannotRunNow) 
    }
    
    toBeRun := remaining.toSet
    
    result
  }*/
}

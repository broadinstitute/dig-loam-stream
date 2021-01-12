package loamstream.drm

import loamstream.util.ValueBox

/**
 * @author clint
 * Jan 11, 2021
 */
trait SessionTracker {
  def register(drmTaskIds: Iterable[DrmTaskId]): Unit
  
  def taskArrayIdsSoFar: Iterable[String]
  
  def isEmpty: Boolean
  
  final def nonEmpty: Boolean = !isEmpty
}

object SessionTracker {
  object Noop extends SessionTracker {
    final override def register(drmTaskIds: Iterable[DrmTaskId]): Unit = ()
  
    final override def taskArrayIdsSoFar: Iterable[String] = Nil
  
    final override def isEmpty: Boolean = true
  }
  
  final class Default extends SessionTracker {
    private[this] val soFar: ValueBox[java.util.Set[String]] = ValueBox(new java.util.HashSet)
    
    import scala.collection.JavaConverters._
    
    override def register(drmTaskIds: Iterable[DrmTaskId]): Unit = soFar.mutate { sf =>
      sf.addAll(drmTaskIds.map(_.jobId).asJavaCollection)
      
      sf
    }
  
    override def taskArrayIdsSoFar: Iterable[String] = soFar.get(_.asScala.toIterable)
    
    override def isEmpty: Boolean = soFar.get(_.isEmpty)
  }
  
  object Default {
    def empty: Default = new Default
  }
}

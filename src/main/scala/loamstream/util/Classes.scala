package loamstream.util

import scala.reflect.ClassTag

/**
 * @author clint
 * Sep 21, 2017
 */
object Classes {
  def simpleNameOf[A: ClassTag]: String = implicitly[ClassTag[A]].runtimeClass.getSimpleName
  
  def simpleNameOf(a: Any): String = a.getClass.getSimpleName
}

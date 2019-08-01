package loamstream.util.code

/**
  * LoamStream
  * Created by oliverr on 5/17/2016.
  */
object ReflectionUtil {

  /** Encodes object name and creates instance of it */
  def getObject[T](classLoader: ClassLoader, scalaObjectId: ObjectId): T = {
    getObject[T](classLoader, scalaObjectId.inJvmFull)
  }

  def getObject[T](classLoader: ClassLoader, jvmFullyQualifiedClassName: String): T = {
    val nullAsDummyValue = null // scalastyle:ignore null
   
    val clazz = classLoader.loadClass(jvmFullyQualifiedClassName)
    
    println(s"Loaded class '${jvmFullyQualifiedClassName}' as $clazz")
    
    val field = clazz.getField("MODULE$")
    
    println(s"Got field $field")
    
    val instance = field.get(nullAsDummyValue).asInstanceOf[T]
    
    println(s"Made instance $instance")
    
    instance
  }
}


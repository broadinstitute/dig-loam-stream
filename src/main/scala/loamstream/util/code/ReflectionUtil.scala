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
    
    val field = clazz.getField("MODULE$")
    
    field.get(nullAsDummyValue).asInstanceOf[T]
  }
}

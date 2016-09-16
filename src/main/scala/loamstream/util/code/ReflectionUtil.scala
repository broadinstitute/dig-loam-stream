package loamstream.util.code

/**
  * LoamStream
  * Created by oliverr on 5/17/2016.
  */
object ReflectionUtil {

  private val nullAsDummyValue = null // scalastyle:ignore null

  /** Encodes object name and creates instance of it */
  def getObject[T](classLoader: ClassLoader, typeName: TypeName): T =
  classLoader.loadClass(typeName.fullNameJvm + "$").getField("MODULE$").get(nullAsDummyValue).asInstanceOf[T]

}


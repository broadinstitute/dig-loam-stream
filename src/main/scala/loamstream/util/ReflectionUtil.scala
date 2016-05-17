package loamstream.util

/**
  * LoamStream
  * Created by oliverr on 5/17/2016.
  */
object ReflectionUtil {

  val nullAsDummyValue = null // scalastyle:ignore null

  def getObject[T](classLoader: ClassLoader, fullName: String): T =
    classLoader.loadClass(fullName + "$").getField("MODULE$").get(nullAsDummyValue).asInstanceOf[T]

}

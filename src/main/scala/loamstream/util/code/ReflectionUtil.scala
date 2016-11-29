package loamstream.util.code

/**
  * LoamStream
  * Created by oliverr on 5/17/2016.
  */
object ReflectionUtil {

  private val nullAsDummyValue = null // scalastyle:ignore null

  /** Encodes object name and creates instance of it */
  def getObject[T](classLoader: ClassLoader, scalaObjectId: ObjectId): T = {
    classLoader.loadClass(scalaObjectId.inJvmFull).getField("MODULE$").get(nullAsDummyValue).asInstanceOf[T]
  }

}


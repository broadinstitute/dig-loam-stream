package loamstream.utils

/**
  * LoamStream
  * Created by oliverr on 3/4/2016.
  */
object TestUtils {

  def assertSomeAndGet[A](option: Option[A]): A = {
    assert(option.nonEmpty)
    option.get
  }

}

package util

/**
  * LoamStream
  * Created by oliverr on 1/27/2016.
  */
trait SizePredictionIterator[+A] extends Iterator[A] {

  def predictSize: Int

}

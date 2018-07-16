package loamstream.util

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.scalatest.FunSuite

import rx.lang.scala.Observable
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.Subject

/**
 * @author clint
 * date: Jul 1, 2016
 */
final class ObservableEnrichmentsTest extends FunSuite {
  //scalastyle:off magic.number
  
  private def isEven(i: Int): Boolean = i % 2 == 0
  private def isOdd(i: Int): Boolean = !isEven(i)
  
  private def waitFor[A](f: Future[A]): A = Await.result(f, Duration.Inf)

  import Observables.Implicits._
  
  
  
  // scalastyle:on magic.number
}

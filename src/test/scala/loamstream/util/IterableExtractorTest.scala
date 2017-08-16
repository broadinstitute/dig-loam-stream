package loamstream.util

import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oruebenacker on 11.08.17.
  */
class IterableExtractorTest extends FunSuite{
  val extractor: IterableExtractor[String, Seq] = IterableExtractor.newFor[String, Seq]
  test("Matches when it should"){
    val originalStrings = Seq("yo", "hello")
    val matchedStrings = Seq("yo", "hello") match {
      case extractor(strings) => strings
    }
    assert(matchedStrings === originalStrings)
  }

}

package loamstream.util

import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oruebenacker on 11.08.17.
  */
class TypedIterableExtractorTest extends FunSuite{
  val stringsExtractor: TypedIterableExtractor[String] = TypedIterableExtractor.newFor[String]
  val anyRefsExtractor: TypedIterableExtractor[AnyRef] = TypedIterableExtractor.newFor[AnyRef]
  val intsExtractor: TypedIterableExtractor[Int] = TypedIterableExtractor.newFor[Int]
  test("Matches only when it should"){
    val strings = Seq("yo", "hello")
    strings match {
      case stringsExtractor(matchedStrings) => assert(matchedStrings === strings)
      case _ => fail("Seq[String] did not match strings extractor.")
    }
    strings match {
      case anyRefsExtractor(matchedStrings) => assert(matchedStrings === strings)
      case _ => fail("Seq[String] did not match anyrefs extractor.")
    }
    strings match {
      case intsExtractor(_) => fail("Seq[Strings] matched ints extractor.")
      case _ => ()
    }
    Seq.empty match {
      case stringsExtractor(matchedStrings) => assert(matchedStrings.isEmpty)
      case _ => fail("Empty seq did not match strings extractor.")
    }
    Seq.empty match {
      case anyRefsExtractor(matchedStrings) => assert(matchedStrings.isEmpty)
      case _ => fail("Empty seq did not match anyrefs extractor.")
    }
    Seq.empty match {
      case intsExtractor(matchedStrings) => assert(matchedStrings.isEmpty)
      case _ => fail("Empty seq did not match ints extractor.")
    }
  }

}

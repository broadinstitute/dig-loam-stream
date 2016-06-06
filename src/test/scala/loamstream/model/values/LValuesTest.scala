package loamstream.model.values

import loamstream.model.values.LType.LTuple.LTuple1
import loamstream.model.values.LType.{LInt, LLong, LSeq, LSet, LString}
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 4/21/2016.
  */
final class LValuesTest extends FunSuite {
  test("Testing LValues") {
    //scalastyle:off magic.number
    assert(LValue(42, LInt) === LInt(42))
    assert(LValue(42L, LLong) === LLong(42))
    assert(LValue("Hello World!", LString) === LString("Hello World!"))
    assert(LValue(Set("Hello", " World!"), LSet(LString)) === LSet(LString).toValue(Set("Hello", " World!")))
    assert(LValue(Seq("Hello", " World!"), LSeq(LString)) === LSeq(LString).toValue(Seq("Hello", " World!")))
    assert(LTuple1(LString).asSeq === Seq(LString))
    assertResult(false)(LInt(42) == LLong(42))
    assertResult(false)(LSet(LString).toValue(Set("Hello", "World")) == LSeq(LString).toValue(Seq("Hello", "World")))
    //scalastyle:on magic.number
  }

}

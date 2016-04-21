package loamstream.model.values

import loamstream.model.values.LType.{LInt, LLong, LSampleId, LSeq, LSet, LString, LVariantId}
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 4/21/2016.
  */
class LValuesTest extends FunSuite {
  test("Testing LValues") {
    //scalastyle:off magic.number
    assert(LValue(42, LInt) === LInt(42))
    assert(LValue(42L, LLong) === LLong(42))
    assert(LValue("Hello World!", LString) === LString("Hello World!"))
    assert(LValue(Set("Hello", " World!"), LSet(LString)) === LSet(LString)(Set("Hello", " World!")))
    assert(LValue(Seq("Hello", " World!"), LSeq(LString)) === LSeq(LString)(Seq("Hello", " World!")))
    assertResult(false)(LInt(42) == LLong(42))
    assertResult(false)(LSet(LString)(Set("Hello", "World")) == LSeq(LString)(Seq("Hello", "World")))
    assertResult(false)(LSampleId("Sample1") == LString("Sample1"))
    assertResult(false)(LVariantId("Sample1") == LString("Sample1"))
    assertResult(false)(LVariantId("Sample1") == LSampleId("Sample1"))
    //scalastyle:on magic.number
  }

}

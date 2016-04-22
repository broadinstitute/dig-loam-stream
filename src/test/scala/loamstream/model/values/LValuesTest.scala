package loamstream.model.values

import loamstream.model.values.LType.LTuple.{LTuple1, LTuple10, LTuple11, LTuple12, LTuple13, LTuple14, LTuple15,
LTuple16, LTuple17, LTuple18, LTuple19, LTuple2, LTuple20, LTuple21, LTuple22, LTuple3, LTuple4, LTuple5, LTuple6,
LTuple7, LTuple8, LTuple9}
import loamstream.model.values.LType.{LBoolean, LDouble, LFloat, LInt, LLong, LSampleId, LSeq, LSet, LShort, LString,
LVariantId}
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
    assert(LTuple1(LString).asSeq === Seq(LString))
    assert(LTuple2(LString, LInt).asSeq === Seq(LString, LInt))
    assert(LTuple3(LString, LInt, LShort).asSeq === Seq(LString, LInt, LShort))
    assert(LTuple4(LString, LInt, LShort, LLong).asSeq === Seq(LString, LInt, LShort, LLong))
    assert(LTuple5(LString, LInt, LShort, LLong, LVariantId).asSeq === Seq(LString, LInt, LShort, LLong, LVariantId))
    assert(LTuple6(LString, LInt, LShort, LLong, LVariantId, LBoolean).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean))
    assert(LTuple7(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat))
    assert(LTuple8(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble))
    assert(LTuple9(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId))
    assert(LTuple10(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString))
    assert(LTuple11(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt))
    assert(LTuple12(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort))
    assert(LTuple13(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong))
    assert(LTuple14(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId))
    assert(LTuple15(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean))
    assert(LTuple16(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat))
    assert(LTuple17(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble))
    assert(LTuple18(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId))
    assert(LTuple19(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString))
    assert(LTuple20(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt))
    assert(LTuple21(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort))
    assert(LTuple22(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort, LLong).asSeq ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort, LLong))
    assertResult(false)(LInt(42) == LLong(42))
    assertResult(false)(LSet(LString)(Set("Hello", "World")) == LSeq(LString)(Seq("Hello", "World")))
    assertResult(false)(LSampleId("Sample1") == LString("Sample1"))
    assertResult(false)(LVariantId("Sample1") == LString("Sample1"))
    assertResult(false)(LVariantId("Sample1") == LSampleId("Sample1"))
    //scalastyle:on magic.number
  }

}

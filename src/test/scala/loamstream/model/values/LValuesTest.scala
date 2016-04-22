package loamstream.model.values

import loamstream.model.values.LType.{LBoolean, LDouble, LFloat, LInt, LLong, LSampleId, LSeq, LSet, LShort, LString,
LTuple, LVariantId}
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
    assert(LTuple(LString).types === Seq(LString))
    assert(LTuple(LString, LInt).types === Seq(LString, LInt))
    assert(LTuple(LString, LInt, LShort).types === Seq(LString, LInt, LShort))
    assert(LTuple(LString, LInt, LShort, LLong).types === Seq(LString, LInt, LShort, LLong))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId).types === Seq(LString, LInt, LShort, LLong, LVariantId))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort).types ===
      Seq(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort,
        LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort))
    assert(LTuple(LString, LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString,
      LInt, LShort, LLong, LVariantId, LBoolean, LFloat, LDouble, LSampleId, LString, LInt, LShort, LLong).types ===
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

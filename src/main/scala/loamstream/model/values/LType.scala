package loamstream.model.values

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LSig
import loamstream.model.Types.{ClusterId, SampleId, SingletonCount, VariantId}
import loamstream.util.{Hit, Shot}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

object LType {

  case object LBoolean extends LType[Boolean]

  case object LDouble extends LType[Double]

  case object LFloat extends LType[Float]

  case object LLong extends LType[Long]

  case object LInt extends LType[Int]

  case object LShort extends LType[Short]

  case object LChar extends LType[Char]

  case object LByte extends LType[Byte]

  case object LString extends LType[String]

  case object LVariantId extends LType[VariantId]

  case object LSampleId extends LType[SampleId]

  case object LGenotype extends LType[Genotype]

  case object LSingletonCount extends LType[SingletonCount]

  case object LClusterId extends LType[ClusterId]

  sealed trait LIterable[E, I <: Iterable[E]] extends LType[I] {
    def elementType: LType[E]
  }

  case class LSet[E](elementType: LType[E]) extends LIterable[E, Set[E]] {
  }

  case class LSeq[E](elementType: LType[E]) extends LIterable[E, Seq[E]] {
  }

  sealed trait LTuple[T <: Product] extends LType[T] {
    def asSeq: Seq[LType[_]]

    override def to[V](v: LType[V]): LSig.Map = LSig.Map(this, v)

    override def toString: String = asSeq.mkString(" & ")
  }

  object LTuple {

    case class LTuple1[T1](type1: LType[T1]) extends LTuple[Tuple1[T1]] {
      override def asSeq: Seq[LType[_]] = Seq(type1)
    }

    case class LTuple2[T1, T2](type1: LType[T1], type2: LType[T2]) extends LTuple[(T1, T2)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2)
    }

    case class LTuple3[T1, T2, T3](type1: LType[T1], type2: LType[T2], type3: LType[T3]) extends LTuple[(T1, T2, T3)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3)
    }

    case class LTuple4[T1, T2, T3, T4](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4])
      extends LTuple[(T1, T2, T3, T4)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3, type4)
    }

    case class LTuple5[T1, T2, T3, T4, T5](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4],
                                           type5: LType[T5])
      extends LTuple[(T1, T2, T3, T4, T5)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3, type4, type5)
    }

    case class LTuple6[T1, T2, T3, T4, T5, T6](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4],
                                               type5: LType[T5], type6: LType[T6])
      extends LTuple[(T1, T2, T3, T4, T5, T6)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3, type4, type5, type6)
    }

    case class LTuple7[T1, T2, T3, T4, T5, T6, T7](type1: LType[T1], type2: LType[T2], type3: LType[T3],
                                                   type4: LType[T4], type5: LType[T5], type6: LType[T6],
                                                   type7: LType[T7])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3, type4, type5, type6, type7)
    }

    case class LTuple8[T1, T2, T3, T4, T5, T6, T7, T8](type1: LType[T1], type2: LType[T2], type3: LType[T3],
                                                       type4: LType[T4], type5: LType[T5], type6: LType[T6],
                                                       type7: LType[T7], type8: LType[T8])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3, type4, type5, type6, type7, type8)
    }

    case class LTuple9[T1, T2, T3, T4, T5, T6, T7, T8, T9](type1: LType[T1], type2: LType[T2], type3: LType[T3],
                                                           type4: LType[T4], type5: LType[T5], type6: LType[T6],
                                                           type7: LType[T7], type8: LType[T8], type9: LType[T9])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9)
    }

    case class LTuple10[T1, T2, T3, T4, T5, T6, T7,
    T8, T9, T10](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5],
                 type6: LType[T6], type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)] {
      override def asSeq: Seq[LType[_]] = Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10)
    }

    case class LTuple11[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10,
    T11](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11)
    }

    case class LTuple12[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11,
    T12](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12)
    }

    case class LTuple13[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12,
    T13](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13)
    }

    case class LTuple14[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13,
    T14](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14)
    }

    case class LTuple15[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14,
    T15](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15)
    }

    case class LTuple16[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15,
    T16](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15], type16: LType[T16])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16)
    }

    case class LTuple17[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16,
    T17](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15], type16: LType[T16],
         type17: LType[T17])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17)
    }

    case class LTuple18[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17,
    T18](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15], type16: LType[T16],
         type17: LType[T17], type18: LType[T18])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18)
    }

    case class LTuple19[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18,
    T19](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15], type16: LType[T16],
         type17: LType[T17], type18: LType[T18], type19: LType[T19])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19)
    }

    case class LTuple20[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19,
    T20](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15], type16: LType[T16],
         type17: LType[T17], type18: LType[T18], type19: LType[T19], type20: LType[T20])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19, type20)
    }

    case class LTuple21[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20,
    T21](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15], type16: LType[T16],
         type17: LType[T17], type18: LType[T18], type19: LType[T19], type20: LType[T20], type21: LType[T21])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20,
        T21)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19, type20, type21)
    }

    case class LTuple22[T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20,
    T21,
    T22](type1: LType[T1], type2: LType[T2], type3: LType[T3], type4: LType[T4], type5: LType[T5], type6: LType[T6],
         type7: LType[T7], type8: LType[T8], type9: LType[T9], type10: LType[T10], type11: LType[T11],
         type12: LType[T12], type13: LType[T13], type14: LType[T14], type15: LType[T15], type16: LType[T16],
         type17: LType[T17], type18: LType[T18], type19: LType[T19], type20: LType[T20], type21: LType[T21],
         type22: LType[T22])
      extends LTuple[(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20,
        T21, T22)] {
      override def asSeq: Seq[LType[_]] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19, type20, type21, type22)
    }

  }

  case class LMap[K, V](keyType: LType[K], valueType: LType[V]) extends LIterable[(K, V), Map[K, V]] {
    override val elementType: LType[(K, V)] = keyType & valueType
  }

}

sealed trait LTypeBase {
  def asEncodeable: Shot[LTypeEncodeableBase]
}

sealed trait LTypeEncodeableBase extends LTypeBase {
  override def asEncodeable: Hit[LTypeEncodeableBase] = Hit(this)
}

sealed trait LTypeEncodeable[T] extends LType[T] {
  override def asEncodeable: Hit[LTypeEncodeable[T]] = Hit(this)
}

sealed trait LType[T] extends LTypeBase {
  def apply(value: T): LValue[T] = LValue(value, this)

  def of(value: T): LValue[T] = apply(value)

  override def asEncodeable: Shot[LTypeEncodeable[T]]

  import LType.LTuple.{LTuple1, LTuple2}

  def &[U](other: LType[U]): LTuple2[T, U] = LTuple2(this, other)

  def to[U](other: LType[U]): LSig.Map = LTuple1(this) to other
}


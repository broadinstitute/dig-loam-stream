package loamstream.model.values

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LSig
import loamstream.model.Types.{ClusterId, SampleId, SingletonCount, VariantId}
import loamstream.model.values.LType.LTuple.LTuple2
import loamstream.util.{Hit, Miss, Shot}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

// scalastyle:off
object LType {

  case object LBoolean extends LTypeAtomic[Boolean]

  case object LDouble extends LTypeAtomic[Double]

  case object LFloat extends LTypeAtomic[Float]

  case object LLong extends LTypeAtomic[Long]

  case object LInt extends LTypeAtomic[Int]

  case object LShort extends LTypeAtomic[Short]

  case object LChar extends LTypeAtomic[Char]

  case object LByte extends LTypeAtomic[Byte]

  case object LString extends LTypeAtomic[String]

  case object LVariantId extends LTypeAtomic[VariantId]

  case object LSampleId extends LTypeAtomic[SampleId]

  case object LGenotype extends LTypeAtomic[Genotype]

  case object LSingletonCount extends LTypeAtomic[SingletonCount]

  case object LClusterId extends LTypeAtomic[ClusterId]

  sealed trait LIterable extends LType {
    def elementType: LType
  }

  sealed trait LIterableEncodeable extends LIterable {
    override def elementType: LType
  }

  object LSet {
    def apply(elementType: LType): LSet = LSetAny(elementType)

    def unapply(set: LSet): Option[LType] = Some(set.elementType)
  }

  sealed trait LSet extends LIterable

  case class LSetAny(elementType: LType) extends LSet

  case class LSetEncodeable(elementType: LType) extends LSet with LIterableEncodeable

  object LSeq {
    def apply(elementType: LType): LSeq = LSeqAny(elementType)

    def unapply(seq: LSeq): Option[LType] = Some(seq.elementType)
  }

  sealed trait LSeq extends LIterable {
  }

  case class LSeqAny(elementType: LType) extends LSeq

  case class LSeqEncodeable(elementType: LType) extends LSeq with LIterableEncodeable

  sealed trait LTuple extends LType {
    def arity: Int

    def asSeq: Seq[LType]

    override def to(v: LType): LSig.Map = LSig.Map(this, v)

    override def toString: String = asSeq.mkString(" & ")

  }

  object LTuple {

    // scalastyle:off cyclomatic.complexity
    def seqToTupleShot(seq: Seq[LType]): Shot[LTuple] = seq match {
      case Seq(e1) => Hit(LTuple1(e1))
      case Seq(e1, e2) => Hit(LTuple2(e1, e2))
      case Seq(e1, e2, e3) => Hit(LTuple3(e1, e2, e3))
      case Seq(e1, e2, e3, e4) => Hit(LTuple4(e1, e2, e3, e4))
      case Seq(e1, e2, e3, e4, e5) => Hit(LTuple5(e1, e2, e3, e4, e5))
      case Seq(e1, e2, e3, e4, e5, e6) => Hit(LTuple6(e1, e2, e3, e4, e5, e6))
      case Seq(e1, e2, e3, e4, e5, e6, e7) => Hit(LTuple7(e1, e2, e3, e4, e5, e6, e7))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8) => Hit(LTuple8(e1, e2, e3, e4, e5, e6, e7, e8))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9) => Hit(LTuple9(e1, e2, e3, e4, e5, e6, e7, e8, e9))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10) => Hit(LTuple10(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11) =>
        Hit(LTuple11(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12) =>
        Hit(LTuple12(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13) =>
        Hit(LTuple13(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14) =>
        Hit(LTuple14(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15) =>
        Hit(LTuple15(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16) =>
        Hit(LTuple16(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17) =>
        Hit(LTuple17(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18) =>
        Hit(LTuple18(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19) =>
        Hit(LTuple19(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20) =>
        Hit(LTuple20(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21) =>
        Hit(LTuple21(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21))
      case Seq(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21, e22) =>
        Hit(LTuple22(e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12, e13, e14, e15, e16, e17, e18, e19, e20, e21,
          e22))
      case _ if seq.isEmpty => Miss("Cannot convert empty seq to tuple.")
      case _ => Miss(s"Cannot convert this seq of size ${seq.size} to tuple.")
    }

    // scalastyle:on cyclomatic.complexity

    case class LTuple1(type1: LType) extends LTuple {
      override val arity: Int = 1

      override def asSeq: Seq[LType] = Seq(type1)
    }

    case class LTuple2(type1: LType, type2: LType) extends LTuple {
      override val arity: Int = 2

      override def asSeq: Seq[LType] = Seq(type1, type2)
    }

    case class LTuple3(type1: LType, type2: LType, type3: LType) extends LTuple {
      override val arity: Int = 3

      override def asSeq: Seq[LType] = Seq(type1, type2, type3)
    }

    case class LTuple4(type1: LType, type2: LType, type3: LType, type4: LType) extends LTuple {
      override val arity: Int = 4

      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4)
    }

    case class LTuple5(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType) extends LTuple {
      override val arity: Int = 5

      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5)
    }

    case class LTuple6(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType)
      extends LTuple {
      override val arity: Int = 6

      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6)
    }

    case class LTuple7(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                       type7: LType)
      extends LTuple {
      override val arity: Int = 7

      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7)
    }

    case class LTuple8(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                       type7: LType, type8: LType)
      extends LTuple {
      override val arity: Int = 8

      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7, type8)
    }

    case class LTuple9(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                       type7: LType, type8: LType, type9: LType)
      extends LTuple {
      override val arity: Int = 9

      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9)
    }

    case class LTuple10(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType)
      extends LTuple {
      override val arity: Int = 10

      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10)
    }

    //scalastyle:off number.of.types
    case class LTuple11(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType)
      extends LTuple {
      override val arity: Int = 11

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11)
    }

    case class LTuple12(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType)
      extends LTuple {
      override val arity: Int = 12

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12)
    }

    case class LTuple13(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType)
      extends LTuple {
      override val arity: Int = 13

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13)
    }

    case class LTuple14(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType)
      extends LTuple {
      override val arity: Int = 14

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14)
    }

    case class LTuple15(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType)
      extends LTuple {
      override val arity: Int = 15

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15)
    }

    case class LTuple16(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType)
      extends LTuple {
      override val arity: Int = 16

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16)
    }

    case class LTuple17(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType)
      extends LTuple {
      override val arity: Int = 17

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17)
    }

    case class LTuple18(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType, type18: LType)
      extends LTuple {
      override val arity: Int = 18

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18)
    }

    case class LTuple19(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType, type18: LType,
                        type19: LType)
      extends LTuple {
      override val arity: Int = 19

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19)
    }

    case class LTuple20(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType, type18: LType,
                        type19: LType, type20: LType)
      extends LTuple {
      override val arity: Int = 20

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19, type20)
    }

    case class LTuple21(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType, type18: LType,
                        type19: LType, type20: LType, type21: LType)
      extends LTuple {
      override val arity: Int = 21

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19, type20, type21)

    }

    case class LTuple22(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType, type18: LType,
                        type19: LType, type20: LType, type21: LType, type22: LType)
      extends LTuple {
      override val arity: Int = 22

      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18, type19, type20, type21, type22)

    }

    //scalastyle:on number.of.types

  }

  object LMap {
    def apply(keyType: LType, valueType: LType): LMap = LMapAny(keyType, valueType)

    def unapply(map: LMap): Option[(LType, LType)] = Some((map.keyType, map.valueType))
  }

  sealed trait LMap extends LIterable {
    def keyType: LType

    def valueType: LType

    override def elementType: LTuple2 = LTuple2(keyType, valueType)
  }

  case class LMapAny(keyType: LType, valueType: LType) extends LMap

  case class LMapEncodeable(keyType: LType, valueType: LType)
    extends LMap with LIterableEncodeable {
    override def elementType: LTuple2 = LTuple2(keyType, valueType)
  }

}

sealed trait LTypeAtomic[T] extends LType {
  def apply(value: T): LValue = LValue(value, this)

  def of(value: T): LValue = apply(value)
}

sealed trait LType {
  def toValue(value: Any): LValue = LValue(value, this)

  import LType.LTuple.{LTuple1, LTuple2}

  def &(other: LType): LTuple2 = LTuple2(this, other)

  def to(other: LType): LSig.Map = LTuple1(this) to other
}

//scalastyle:on

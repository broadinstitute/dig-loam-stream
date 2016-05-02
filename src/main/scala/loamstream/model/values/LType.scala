package loamstream.model.values

import htsjdk.variant.variantcontext.Genotype
import loamstream.model.LSig
import loamstream.model.Types.{ClusterId, SampleId, SingletonCount, VariantId}
import loamstream.util.{Hit, Miss, Shot}

/**
  * RugLoom - A prototype for a pipeline building toolkit
  * Created by oruebenacker on 2/18/16.
  */

object LType {

  case object LBoolean extends LTypeAtomic[Boolean] with LTypeEncodeable

  case object LDouble extends LTypeAtomic[Double] with LTypeEncodeable

  case object LFloat extends LTypeAtomic[Float] with LTypeEncodeable

  case object LLong extends LTypeAtomic[Long] with LTypeEncodeable

  case object LInt extends LTypeAtomic[Int] with LTypeEncodeable

  case object LShort extends LTypeAtomic[Short] with LTypeEncodeable

  case object LChar extends LTypeAtomic[Char] with LTypeEncodeable

  case object LByte extends LTypeAtomic[Byte] with LTypeEncodeable

  case object LString extends LTypeAtomic[String] with LTypeEncodeable

  case object LVariantId extends LTypeAtomic[VariantId] with LTypeEncodeable

  case object LSampleId extends LTypeAtomic[SampleId] with LTypeEncodeable

  case object LGenotype extends LTypeAtomic[Genotype]

  case object LSingletonCount extends LTypeAtomic[SingletonCount] with LTypeEncodeable

  case object LClusterId extends LTypeAtomic[ClusterId] with LTypeEncodeable

  sealed trait LIterable extends LType {
    def elementType: LType

    override def isEncodeable: Boolean = elementType.isEncodeable
  }

  case class LSet(elementType: LType) extends LIterable {
    override def asEncodeable: Shot[LSet] =
      if (isEncodeable) {
        Hit(this)
      } else {
        Miss(s"Set is not encodeable, because element type '$elementType' is not encodeable.")
      }
  }

  case class LSeq(elementType: LType) extends LIterable {
    override def asEncodeable: Shot[LSeq] =
      if (isEncodeable) {
        Hit(this)
      } else {
        Miss(s"Seq is not encodeable, because element type '$elementType' is not encodeable.")
      }
  }

  sealed trait LTuple extends LType {
    def arity: Int

    def asSeq: Seq[LType]

    override def isEncodeable: Boolean = asSeq.forall(_.isEncodeable)

    override def to(v: LType): LSig.Map = LSig.Map(this, v)

    override def toString: String = asSeq.mkString(" & ")

  }

  object LTuple {

    def isEncodeable[Tup <: LTuple](tuple: Tup): Shot[Tup] = if (tuple.isEncodeable) {
      Hit(tuple)
    } else {
      val className = tuple.getClass.getSimpleName
      val unencodeables = tuple.asSeq.filterNot(_.isEncodeable)
      if (unencodeables.size == 1) {
        Miss(s"Cannot encode $className, because element '${unencodeables.head}' is not encodeable.")
      } else {
        Miss(s"Cannot encode $className, because elements ${unencodeables.mkString("'", ", ", "'")} " +
          "are not encodeable.")
      }
    }

    case class LTuple1(type1: LType) extends LTuple {
      override val arity: Int = 1
      override def asSeq: Seq[LType] = Seq(type1)

      override def asEncodeable: Shot[LTuple1] = LTuple.isEncodeable(this)
    }

    case class LTuple2(type1: LType, type2: LType) extends LTuple {
      override val arity: Int = 2
      override def asSeq: Seq[LType] = Seq(type1, type2)

      override def asEncodeable: Shot[LTuple2] = LTuple.isEncodeable(this)
    }

    case class LTuple3(type1: LType, type2: LType, type3: LType) extends LTuple {
      override val arity: Int = 3
      override def asSeq: Seq[LType] = Seq(type1, type2, type3)

      override def asEncodeable: Shot[LTuple3] = LTuple.isEncodeable(this)
    }

    case class LTuple4(type1: LType, type2: LType, type3: LType, type4: LType) extends LTuple {
      override val arity: Int = 4
      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4)

      override def asEncodeable: Shot[LTuple4] = LTuple.isEncodeable(this)
    }

    case class LTuple5(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType) extends LTuple {
      override val arity: Int = 5
      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5)

      override def asEncodeable: Shot[LTuple5] = LTuple.isEncodeable(this)
    }

    case class LTuple6(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType)
      extends LTuple {
      override val arity: Int = 6
      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6)

      override def asEncodeable: Shot[LTuple6] = LTuple.isEncodeable(this)
    }

    case class LTuple7(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                       type7: LType)
      extends LTuple {
      override val arity: Int = 7
      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7)

      override def asEncodeable: Shot[LTuple7] = LTuple.isEncodeable(this)
    }

    case class LTuple8(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                       type7: LType, type8: LType)
      extends LTuple {
      override val arity: Int = 8
      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7, type8)

      override def asEncodeable: Shot[LTuple8] = LTuple.isEncodeable(this)
    }

    case class LTuple9(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                       type7: LType, type8: LType, type9: LType)
      extends LTuple {
      override val arity: Int = 9
      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9)

      override def asEncodeable: Shot[LTuple9] = LTuple.isEncodeable(this)
    }

    case class LTuple10(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType)
      extends LTuple {
      override val arity: Int = 10
      override def asSeq: Seq[LType] = Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10)

      override def asEncodeable: Shot[LTuple10] = LTuple.isEncodeable(this)
    }

    case class LTuple11(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType)
      extends LTuple {
      override val arity: Int = 11
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11)

      override def asEncodeable: Shot[LTuple11] = LTuple.isEncodeable(this)
    }

    case class LTuple12(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType)
      extends LTuple {
      override val arity: Int = 12
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12)

      override def asEncodeable: Shot[LTuple12] = LTuple.isEncodeable(this)
    }

    case class LTuple13(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType)
      extends LTuple {
      override val arity: Int = 13
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13)

      override def asEncodeable: Shot[LTuple13] = LTuple.isEncodeable(this)
    }

    case class LTuple14(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType)
      extends LTuple {
      override val arity: Int = 14
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14)

      override def asEncodeable: Shot[LTuple14] = LTuple.isEncodeable(this)
    }

    case class LTuple15(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType)
      extends LTuple {
      override val arity: Int = 15
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15)

      override def asEncodeable: Shot[LTuple15] = LTuple.isEncodeable(this)
    }

    case class LTuple16(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType)
      extends LTuple {
      override val arity: Int = 16
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16)

      override def asEncodeable: Shot[LTuple16] = LTuple.isEncodeable(this)
    }

    case class LTuple17(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType)
      extends LTuple {
      override val arity: Int = 17
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17)

      override def asEncodeable: Shot[LTuple17] = LTuple.isEncodeable(this)
    }

    case class LTuple18(type1: LType, type2: LType, type3: LType, type4: LType, type5: LType, type6: LType,
                        type7: LType, type8: LType, type9: LType, type10: LType, type11: LType, type12: LType,
                        type13: LType, type14: LType, type15: LType, type16: LType, type17: LType, type18: LType)
      extends LTuple {
      override val arity: Int = 18
      override def asSeq: Seq[LType] =
        Seq(type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14,
          type15, type16, type17, type18)

      override def asEncodeable: Shot[LTuple18] = LTuple.isEncodeable(this)
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

      override def asEncodeable: Shot[LTuple19] = LTuple.isEncodeable(this)
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

      override def asEncodeable: Shot[LTuple20] = LTuple.isEncodeable(this)
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

      override def asEncodeable: Shot[LTuple21] = LTuple.isEncodeable(this)
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

      override def asEncodeable: Shot[LTuple22] = LTuple.isEncodeable(this)
    }

  }

  case class LMap(keyType: LType, valueType: LType) extends LIterable {
    override val elementType: LType = keyType & valueType

    override def isEncodeable: Boolean = elementType.isEncodeable

    override def asEncodeable: Shot[LSeq] =
      if (!keyType.isEncodeable) {
        Miss(s"Map is not encodeable, because key type '$keyType' is not encodeable.")
      } else if (!valueType.isEncodeable) {
        Miss(s"Map is not encodeable, because value type '$valueType' is not encodeable.")
      } else {
        Hit(this)
      }
  }

}

sealed trait LTypeEncodeable extends LType {
  override def isEncodeable: Boolean = true

  override def asEncodeable: Hit[LTypeEncodeable] = Hit(this)
}

sealed trait LTypeAtomic[T] extends LType {
  def apply(value: T): LValue = LValue(value, this)

  def of(value: T): LValue = apply(value)
}

sealed trait LType {
  def isEncodeable: Boolean = false

  def asEncodeable: Shot[LTypeEncodeable] = Miss(s"Type ${getClass.getSimpleName} is not encodeable.")

  import LType.LTuple.{LTuple1, LTuple2}

  def &(other: LType): LTuple2 = LTuple2(this, other)

  def to(other: LType): LSig.Map = LTuple1(this) to other
}


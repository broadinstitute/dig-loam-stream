package loamstream.map

import loamstream.map.Mapping.{Constraint, Slot, Target, TargetGenerator}

/**
  * LoamStream
  * Created by oliverr on 1/22/2016.
  */
object Mapping {

  trait Target

  trait SlotBase

  trait Slot[T <: Target] extends SlotBase

  trait TargetGeneratorBase {
    def slot: SlotBase

    def numMoreTargets: Int

    def nextTarget: Target
  }

  trait TargetGenerator[T <: Target] extends TargetGeneratorBase {
    def slot: Slot[T]

    def nextTarget: T
  }

  trait Constraint {
    def slots: Set[Slot[_]]

    def applyToSlotUntyped(mapping: Mapping, slot: SlotBase, generator: TargetGeneratorBase): TargetGeneratorBase

    def applyToSlot[T](mapping: Mapping, slot: Slot[T], generator: TargetGenerator[T]): TargetGenerator[T] =
      applyToSlotUntyped(mapping, slot, generator).asInstanceOf[TargetGenerator[T]]

    def applyToMapping(mapping: Mapping): Mapping = {
      var generatorsNew = mapping.generators
      for (slot <- slots) {
        mapping.generators.get(slot) match {
          case Some(generator) =>
            generatorsNew += slot -> applyToSlotUntyped(mapping, slot, generator)
          case None => ()
        }
      }
      mapping.copy(generators = generatorsNew)
    }
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Map.empty)

  def fromSlots(slots: Map[Slot[_], TargetGenerator[_]]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Map.empty)

}

case class Mapping(slots: Set[Slot[_]], emptySlots: Set[Slot[_]], generators: Map[Slot[_], TargetGenerator[_]],
                   constraints: Set[Constraint], bindings: Map[Slot[_], Target]) {

  def plusSlot[T <: Target](slot: Slot[T], generator: TargetGenerator[T]) =
    copy(slots = slots + slot, emptySlots = emptySlots + slot, generators = generators + (slot -> generator))

  def constraintRefreshed(constraint: Constraint) = constraint.applyToMapping(this)

  def allConstraintsRefreshed = {
    var mapping = this
    for (constraint <- constraints) {
      mapping = mapping.constraintRefreshed(constraint)
    }
    mapping
  }

  def plusConstraint(constraint: Constraint) =
    copy(constraints = constraints + constraint).constraintRefreshed(constraint)

  def plusBinding[T <: Target](slot: Slot[T], target: T) =
    copy(emptySlots = emptySlots - slot, bindings = bindings + (slot -> target))

}

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

    def constrainBase(mapping: Mapping, slot: SlotBase, generator: TargetGeneratorBase): TargetGeneratorBase

    def constrain[T](mapping: Mapping, slot: Slot[T], generator: TargetGenerator[T]): TargetGenerator[T] =
      constrainBase(mapping, slot, generator).asInstanceOf[TargetGenerator[T]]

    def constrainAll(mapping: Mapping): Mapping = {
      var generatorsNew = mapping.generators
      for (slot <- slots) {
        mapping.generators.get(slot) match {
          case Some(generator) =>
            generatorsNew += slot -> constrainBase(mapping, slot, generator)
          case None => ()
        }
      }
      mapping.copy(generators = generatorsNew)
    }
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Map.empty, None)

  def fromSlots(slots: Map[Slot[_], TargetGenerator[_]]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Map.empty, None)

}

case class Mapping(slots: Set[Slot[_]], emptySlots: Set[Slot[_]], generators: Map[Slot[_], TargetGenerator[_]],
                   constraints: Set[Constraint], bindings: Map[Slot[_], Target], boundFromOpt: Option[Mapping]) {

  def withSlot[T <: Target](slot: Slot[T], generator: TargetGenerator[T]) =
    copy(slots = slots + slot, emptySlots = emptySlots + slot, generators = generators + (slot -> generator),
      boundFromOpt = None)

  def withConstraint(constraint: Constraint) = constraint.constrainAll(copy(constraints = constraints + constraint))

  def withBinding[T <: Target](slot: Slot[T], target: T) =
    copy(emptySlots = emptySlots - slot, bindings = bindings + (slot -> target), boundFromOpt = Some(this))

}

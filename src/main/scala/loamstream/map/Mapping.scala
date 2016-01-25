package loamstream.map

import loamstream.map.Mapping.{Constraint, Slot, Target, TargetGenerator}

/**
  * LoamStream
  * Created by oliverr on 1/22/2016.
  */
object Mapping {

  trait Target

  trait Slot

  trait TargetGenerator {
    def slot: Slot

    def numMoreTargets: Int

    def nextTarget: Target
  }

  trait Constraint {
    def slots: Set[Slot]

    def applyToSlot(mapping: Mapping, slot: Slot, generator: TargetGenerator): TargetGenerator

    def applyToMapping(mapping: Mapping): Mapping = {
      var generatorsNew = mapping.generators
      for (slot <- slots) {
        mapping.generators.get(slot) match {
          case Some(generator) =>
            generatorsNew += slot -> applyToSlot(mapping, slot, generator)
          case None => ()
        }
      }
      mapping.copy(generators = generatorsNew)
    }
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Map.empty)

  def fromSlots(slots: Map[Slot, TargetGenerator]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Map.empty)

}

case class Mapping(slots: Set[Slot], emptySlots: Set[Slot], generators: Map[Slot, TargetGenerator],
                   constraints: Set[Constraint], bindings: Map[Slot, Target]) {

  def plusSlot(slot: Slot, generator: TargetGenerator) =
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

  def plusBinding(slot: Slot, target: Target) =
    copy(emptySlots = emptySlots - slot, bindings = bindings + (slot -> target))

}

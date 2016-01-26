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
    def numMoreTargets: Int

    def nextTarget: Target

    def withConstraint(constraint: Constraint): TargetGenerator
  }

  trait Constraint {
    def slots: Set[Slot]
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Map.empty)

  def fromSlots(slots: Map[Slot, TargetGenerator]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Map.empty)

}

case class Mapping(slots: Set[Slot], emptySlots: Set[Slot], generators: Map[Slot, TargetGenerator],
                   constraints: Set[Constraint], bindings: Map[Slot, Target]) {

  def plusSlot(slot: Slot, generator: TargetGenerator) =
    copy(slots = slots + slot, emptySlots = emptySlots + slot, generators = generators + (slot -> generator))

  def plusConstraint(constraint: Constraint) = {
    val constrainedGenerators = constraint.slots.map(slot => (slot, generators(slot).withConstraint(constraint))).toMap
    copy(constraints = constraints + constraint, generators = generators ++ constrainedGenerators)
  }

  def plusBinding(slot: Slot, target: Target) =
    copy(emptySlots = emptySlots - slot, bindings = bindings + (slot -> target))

}

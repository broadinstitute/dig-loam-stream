package loamstream.map

import loamstream.map.Mapping._

/**
  * LoamStream
  * Created by oliverr on 1/22/2016.
  */
object Mapping {

  trait Target

  trait Slot

  trait Rule {
    def slots(slots: Set[Slot]): Set[Slot]

    def slotFilter(slots: Set[Slot], bindings: Map[Slot, Target], slot: Slot): Target => Boolean

    def constraintFor(slots: Set[Slot], bindings: Map[Slot, Target]): Constraint
  }

  trait Constraint {
    def rule: Rule

    def slots: Set[Slot]

    def bindings: Map[Slot, Target]

    def slotFilter(slot: Slot) = rule.slotFilter(slots, bindings, slot: Slot)
  }

  trait RawChoices {
    def constrainedBy(constraints: Set[Constraint]): ConstrainedChoices
  }

  trait ConstrainedChoices {
    def countChoices: Int

    def nextChoice: Target
  }

  def empty = Mapping(Set.empty, Set.empty, Map.empty, Set.empty, Set.empty, Map.empty)

  def fromSlots(slots: Map[Slot, RawChoices]) =
    Mapping(slots.keySet, slots.keySet, slots, Set.empty, Set.empty, Map.empty)

}

case class Mapping(slots: Set[Slot], emptySlots: Set[Slot], choices: Map[Slot, RawChoices],
                   rules: Set[Rule], constraints: Set[Constraint], bindings: Map[Slot, Target]) {

  def plusSlot(slot: Slot, slotChoices: RawChoices) = {
    def slotsNew = slots + slot
    copy(slots = slotsNew, emptySlots = emptySlots + slot,
      constraints = rules.map(_.constraintFor(slotsNew, bindings)), choices = choices + (slot -> slotChoices))
  }

  def plusSlots(slotsChoices: Map[Slot, RawChoices]) = {
    val slotsNew = slots ++ slotsChoices.keys
    copy(slots = slotsNew, emptySlots = emptySlots ++ slotsChoices.keys,
      constraints = rules.map(_.constraintFor(slotsNew, bindings)), choices = choices ++ slotsChoices)
  }

  def plusRule(rule: Rule) =
    copy(rules = rules + rule, constraints = constraints + rule.constraintFor(slots, bindings))

  def plusRules(rulesNew: Iterable[Rule]) =
    copy(rules = rules ++ rulesNew, constraints = constraints ++ rulesNew.map(_.constraintFor(slots, bindings)))

  def plusBinding(slot: Slot, target: Target) = {
    val bindingsNew = bindings + (slot -> target)
    copy(emptySlots = emptySlots - slot, constraints = rules.map(_.constraintFor(slots, bindings)),
      bindings = bindingsNew)
  }

}

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
    def slots(mapping: Mapping): Set[Slot]

    def slotFilter(mapping: Mapping, slot: Slot): Target => Boolean

    def constraintFor(mapping: Mapping): Constraint
  }

  trait Constraint {
    def rule: Rule

    def mapping: Mapping

    def slots: Set[Slot] = rule.slots(mapping)

    def slotFilter(slot: Slot) = rule.slotFilter(mapping: Mapping, slot: Slot)
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

  def plusSlot(slot: Slot, slotChoices: RawChoices) =
    copy(slots = slots + slot, emptySlots = emptySlots + slot, constraints = rules.map(_.constraintFor(this)),
      choices = choices + (slot -> slotChoices))

  def plusSlots(slotsNew: Map[Slot, RawChoices]) =
    copy(slots = slots ++ slotsNew.keys, emptySlots = emptySlots ++ slotsNew.keys,
      constraints = rules.map(_.constraintFor(this)), choices = choices ++ slotsNew)

  def plusRule(rule: Rule) =
    copy(rules = rules + rule, constraints = constraints + rule.constraintFor(this))

  def plusRules(rulesNew: Iterable[Rule]) =
    copy(rules = rules ++ rulesNew, constraints = constraints ++ rulesNew.map(_.constraintFor(this)))

  def plusBinding(slot: Slot, target: Target) =
    copy(emptySlots = emptySlots - slot, constraints = rules.map(_.constraintFor(this)),
      bindings = bindings + (slot -> target))

}

package loamstream.map

import loamstream.map.Mapping.{Constraint, RawChoices, Rule, Slot, Target}
import loamstream.map.SudokuBoard.{IntTarget, SlotXY}
import util.Iterative

/**
  * LoamStream
  * Created by oliverr on 1/26/2016.
  */
object SudokuBoard {

  case class SlotXY(x: Int, y: Int) extends Slot

  case class IntTarget(value: Int) extends Target

  val targets = (1 to 9).map(IntTarget)

  object AllNumbers extends RawChoices {
    override def constrainedBy(slot: Slot, slotConstraints: Set[Constraint]): Iterative.SizePredicting[Target] = {
      var remainingTargets = targets.toSet
      for (slotConstraint <- slotConstraints) {
        remainingTargets = remainingTargets.filter(slotConstraint.slotFilter(slot))
      }
      Iterative.SetBased(remainingTargets)
    }
  }

  case class UniquenessRule(slots: Set[Slot]) extends Rule {
    override def constraintFor(slots: Set[Slot], bindings: Map[Slot, Target]): Constraint =
      UniquenessConstraint(this.slots, targets.toSet -- this.slots.flatMap(bindings.get))
  }

  case class UniquenessConstraint(slots: Set[Slot], remainingTargets: Set[Target]) extends Constraint {
    override def slotFilter(slot: Slot): (Target) => Boolean = remainingTargets.contains
  }

  val xRange = 1 to 9
  val yRange = 1 to 9

  val slots = for (x <- xRange; y <- yRange) yield SlotXY(x, y)

  val unboundMapping = Mapping.fromSlots(slots.map(slot => (slot, AllNumbers)).toMap)

  def print(target: Target): String = {
    target match {
      case IntTarget(value) => value.toString
      case _ => "."
    }
  }

  def row(y: Int) = xRange.map(SlotXY(_, y))

  def column(x: Int) = yRange.map(SlotXY(x, _))

  def sector(ix: Int, iy: Int) = {
    val xMin = 3 * ix - 2
    val yMin = 3 * iy - 2
    for (x <- xMin to xMin + 2; y <- yMin to yMin + 2) yield SlotXY(x, y)
  }

  def slotUniquenessGroups = (for (y <- yRange) yield row(y)) ++ (for (x <- xRange) yield column(x)) ++
    (for (ix <- 1 to 3; iy <- 1 to 3) yield sector(ix, iy))

  def uniquenessRules = slotUniquenessGroups.map(slotGroup => UniquenessRule(slotGroup.toSet))
}

class SudokuBoard {

  var mapping = SudokuBoard.unboundMapping.plusRules(SudokuBoard.uniquenessRules)

  def set(x: Int, y: Int, value: Int): Unit = {
    mapping = mapping.plusBinding(SlotXY(x, y), IntTarget(value))
  }

  def get(x: Int, y: Int): Option[Int] = {
    mapping.bindings.get(SlotXY(x, y)) match {
      case Some(IntTarget(value)) => Some(value)
      case _ => None
    }
  }

  def getChoices(x: Int, y: Int): Set[Int] =
    mapping.choices(SlotXY(x, y)).toSeq.collect({ case IntTarget(value) => value }).toSet

  def cellToString(x: Int, y: Int) = get(x, y).map(_.toString).getOrElse(".")

  override def toString: String = {
    SudokuBoard.yRange.map({ y =>
      SudokuBoard.xRange.map(x => cellToString(x, y)).mkString
    }).mkString("\n")
  }


}

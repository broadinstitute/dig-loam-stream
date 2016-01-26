package loamstream.map

import loamstream.map.Mapping.{ConstrainedChoices, Constraint, RawChoices, Slot, Target}
import loamstream.map.SudokuBoard.{IntTarget, SlotXY}

/**
  * LoamStream
  * Created by oliverr on 1/26/2016.
  */
object SudokuBoard {

  case class SlotXY(x: Int, y: Int) extends Slot

  case class IntTarget(value: Int) extends Target

  object AllNumbers extends RawChoices {
    override def constrainedBy(slot: Slot, constraints: Set[Constraint]): ConstrainedChoices = ???
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

}

class SudokuBoard {

  var mapping = SudokuBoard.unboundMapping

  def set(x: Int, y: Int, value: Int): Unit = {
    mapping = mapping.plusBinding(SlotXY(x, y), IntTarget(value))
  }

  def get(x: Int, y: Int): Option[Int] = {
    mapping.bindings.get(SlotXY(x, y)) match {
      case Some(IntTarget(value)) => Some(value)
      case _ => None
    }
  }

  def cellToString(x: Int, y: Int) = get(x, y).map(_.toString).getOrElse(".")

  override def toString: String = {
    SudokuBoard.yRange.map({ y =>
      SudokuBoard.xRange.map(x => cellToString(x, y)).mkString
    }).mkString("\n")
  }


}

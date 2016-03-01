package loamstream.map

import loamstream.map.MapMaker.Consumer

/**
  * LoamStream
  * Created by oliverr on 1/26/2016.j
  */
object MappingTestApp extends App {

  class TestConsumer(val nStepsMax: Int) extends Consumer {
    var nSteps = 0
    var sudoku = new SudokuBoard
    var solutions = Set.empty[AriadneNode]

    def print(node: AriadneNode): Unit = {
      sudoku.mapping = node.mapping
      println(sudoku)
    }

    override def wantsMore: Boolean = nSteps < nStepsMax

    override def solution(node: AriadneNode): Unit = {
      println("=Solution!=")
      print(node)
      solutions += node
    }

    override def step(node: AriadneNode): Unit = {
      println()
      print(node)
      nSteps += 1
    }

    override def end(): Unit = {
      println("Number of solutions: " + solutions.size)
      for (solution <- solutions) {
        println("=Solution!=")
        print(solution)
      }
    }
  }

  val sudoku = new SudokuBoard

  case class Pos(x:Int, y:Int)
  case class Entry(pos:Pos, value: Int)

  val x1 = 1
  val y1 = 1
  val value1 = 5
  val entry1 = Entry(Pos(x1, y1), value1)
  val x2 = 6
  val y2 = 2
  val value2 = 7
  val entry2 = Entry(Pos(x2, y2), value2)
  sudoku.set(entry1.pos.x, entry1.pos.y, entry1.value)
  sudoku.set(entry2.pos.x, entry2.pos.y, entry2.value)
  println(sudoku)
  val x3 = 6
  val y3 = 1
  val pos3 = Pos(x3, y3)
  println("Choices for " + pos3 + ": " + sudoku.getChoices(pos3.x, pos3.y))
  val nStepsMax: Int = 140
  val consumer = new TestConsumer(nStepsMax)
  MapMaker.traverse(sudoku.mapping, consumer)
  println("Yo, mapper!")

}

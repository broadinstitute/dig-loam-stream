package loamstream.map

import loamstream.map.MapMaker.Consumer

/**
  * LoamStream
  * Created by oliverr on 1/26/2016.
  */
object MappingTestApp extends App {

  class TestConsumer(val nStepsMax: Int) extends Consumer {
    var nSteps = 0
    var sudoku = new SudokuBoard
    var solutions = Set.empty[AriadneNode]

    def print(node: AriadneNode) = {
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
  sudoku.set(1, 1, 5)
  sudoku.set(6, 2, 7)
  println(sudoku)
  println("Choices for (6,1): " + sudoku.getChoices(6, 1))
  val consumer = new TestConsumer(140)
  MapMaker.traverse(sudoku.mapping, consumer)
  println("Yo, mapper!")

}

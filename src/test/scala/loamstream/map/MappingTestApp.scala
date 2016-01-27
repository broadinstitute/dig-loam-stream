package loamstream.map

import util.shot.{Miss, Hit}

/**
  * LoamStream
  * Created by oliverr on 1/26/2016.
  */
object MappingTestApp extends App {

  val sudoku = new SudokuBoard
  sudoku.set(1, 1, 5)
  sudoku.set(6, 2, 7)
  println(sudoku)
  println("Choices for (6,1): " + sudoku.getChoices(6, 1))
  MapMaker.findBinding(sudoku.mapping) match {
    case Hit(mapping) => sudoku.mapping = mapping; println(sudoku)
    case Miss(snag) => println(snag)
  }
  println("Yo, mapper!")

}

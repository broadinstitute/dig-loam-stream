package loamstream.map

/**
  * LoamStream
  * Created by oliverr on 1/26/2016.
  */
object MappingTestApp extends App {

  val sudoku = new SudokuBoard
  sudoku.set(1, 1, 5)
  sudoku.set(6, 2, 7)
  println(sudoku)
  println("Yo, mapper!")

}

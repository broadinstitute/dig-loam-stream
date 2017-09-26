package loamstream.util.lines

/**
  * LoamStream
  * Created by oliverr on 9/26/2017.
  */
case class Lines(lines: Seq[String]) {

  import Lines._

  def indented: Lines = Lines(lines.map(_ + indentation))

  def asString: String = lines.mkString("\n", "\n", "\n")

  def ++(other: Lines): Lines = Lines(lines ++ other.lines)

  def :+(line: String): Lines = Lines(lines :+ line)

  def +:(line: String): Lines = Lines(line +: lines)

}

object Lines {

  val empty: Lines = Lines(Seq.empty)
  val indentation: String = "  "

  def apply(line: String, lines: String*): Lines = Lines(line +: lines)

  trait Printer[-T] {
    def toLines(thing: T): Lines
  }

  def toLines[T](thing: T)(implicit printer: Printer[T]): Lines = printer.toLines(thing)

  case class IterablePrinter[E](elementPrinter: Printer[E]) extends Printer[Iterable[E]] {
    override def toLines(iterable: Iterable[E]): Lines = {
      val elementLines = iterable.map(elementPrinter.toLines).fold(empty)(_ ++ _)
      s"${iterable.getClass.getName}(" +: elementLines :+ ")"
    }
  }

  implicit def iterablePrinter[E](implicit elementPrinter: Printer[E]): IterablePrinter[E] =
    IterablePrinter[E](elementPrinter)

}

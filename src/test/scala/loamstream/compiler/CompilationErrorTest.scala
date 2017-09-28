package loamstream.compiler

import org.scalatest.FunSuite
import scala.reflect.internal.util.Position
import scala.reflect.internal.util.SourceFile
import scala.reflect.io.AbstractFile
import loamstream.compiler.Issue.Severity

/**
 * @author clint
 * Aug 15, 2017
 */
final class CompilationErrorTest extends FunSuite {
  import CompilationErrorTest.Positions
  
  private def issue(pos: Position, msg: String = "foo") = Issue(pos, msg, Severity.Info)
  
  test("hasSourceAndPosition") {
    import CompilationError.hasSourceAndPosition
    import Positions._
    
    assert(hasSourceAndPosition(issue(notDefinedWithContent)) === false)
    
    assert(hasSourceAndPosition(issue(notDefinedNoContent)) === false)
    
    assert(hasSourceAndPosition(issue(isDefinedWithContent)) === true)
    
    assert(hasSourceAndPosition(issue(isDefinedNoContent)) === false)
  }
  
  test("from - HasPosition") {
    import CompilationError.from
    import Positions._
    
    val message = "asldjaslkdjlkasdj"
    val line = "askldjlkasdjklasdj"
    val column = 42
    val sourceFile = "foo.scala"
    
    val expected = CompilationError.HasPosition(line, "foo.loam", 41, message)
    
    assert(from(issue(Positions.from(line, column, sourceFile), message)) === expected)
  }
  
  test("from - NoPosition") {
    import CompilationError.from
    import Positions._
    
    val message = "asdjklalsdkjlasdjlaskdjlka"
    
    assert(from(issue(notDefinedWithContent, message)) === CompilationError.NoPosition(message))
    
    assert(from(issue(notDefinedNoContent, message)) === CompilationError.NoPosition(message))
    
    assert(from(issue(isDefinedNoContent, message)) === CompilationError.NoPosition(message))
  }
  
  test("toHumanReadableString - HasPosition") {
    val message = "asldjaslkdjlkasdj"
    val line = "askldjlkasdjklasdj"
    
    val error = CompilationError.HasPosition(line, "foo.loam", 5, message)
    
    val newLine = System.lineSeparator
    
    val expected = s"foo.loam: 'asldjaslkdjlkasdj'${newLine}askldjlkasdjklasdj${newLine}     ^"
    
    assert(error.toHumanReadableString === expected)
  }
  
  test("toHumanReadableString - NoPosition") {
    val error = CompilationError.NoPosition("asldjaslkdjlkasdj")
    
    val expected = s"(unknown source file): 'asldjaslkdjlkasdj'"
    
    assert(error.toHumanReadableString === expected)
  }
}

object CompilationErrorTest {
  private object Positions {
    def from(sourceLine: String, col: Int, sourcePath: String): Position = new Position {
      override def isDefined: Boolean = true
      override def source: SourceFile = StubSourceFile.withSourcePath(sourcePath)
      override def lineContent: String = sourceLine
      override def column: Int = col
    }
    
    val notDefinedWithContent: Position = new Position {
      override def isDefined: Boolean = false
      override def source: SourceFile = StubSourceFile.withContent("asdf".toArray)
    }
    
    val notDefinedNoContent: Position = new Position {
      override def isDefined: Boolean = false
      override def source: SourceFile = StubSourceFile.withContent(Array.empty[Char])
    }
    
    val isDefinedWithContent: Position = new Position {
      override def isDefined: Boolean = true
      override def source: SourceFile = StubSourceFile.withContent("asdf".toArray)
    }
    
    val isDefinedNoContent: Position = new Position {
      override def isDefined: Boolean = true
      override def source: SourceFile = StubSourceFile.withContent(Array.empty[Char])
    }
  }
  
  private object StubSourceFile {
    def withContent(c: Array[Char]): StubSourceFile = new StubSourceFile {
      override def content: Array[Char] = c
    }
    
    def withSourcePath(p: String): StubSourceFile = new StubSourceFile {
      override def content: Array[Char] = "asdf".toArray
      override def path: String = p
    }
  }
  
  private class StubSourceFile extends SourceFile {
    override def content: Array[Char] = ???
    override def file : AbstractFile = ???
    override def isLineBreak(idx: Int): Boolean = ???
    override def isEndOfLine(idx: Int): Boolean = ???
    override def isSelfContained: Boolean = ???
    override def length : Int = ???
    override def offsetToLine(offset: Int): Int = ???
    override def lineToOffset(index : Int): Int = ???
  }
}

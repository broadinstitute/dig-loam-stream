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
  
  test("hasSourcePosition") {
    import CompilationError.hasSourcePosition
    import Positions._
    
    assert(hasSourcePosition(issue(notDefinedWithContent)) === false)
    
    assert(hasSourcePosition(issue(notDefinedNoContent)) === false)
    
    assert(hasSourcePosition(issue(isDefinedWithContent)) === true)
    
    assert(hasSourcePosition(issue(isDefinedNoContent)) === false)
  }
  
  test("from") {
    import CompilationError.from
    import Positions._
    
    assert(from(issue(notDefinedWithContent)) === None)
    
    assert(from(issue(notDefinedNoContent)) === None)
    
    assert(from(issue(isDefinedNoContent)) === None)
    
    val message = "asldjaslkdjlkasdj"
    val line = "askldjlkasdjklasdj"
    val column = 42
    val sourceFile = "foo.scala"
    
    val expected = CompilationError(line, "foo.loam", 41, message)
    
    assert(from(issue(Positions.from(line, column, sourceFile), message)) === Some(expected))
  }
  
  test("toHumanReadableString") {
    val message = "asldjaslkdjlkasdj"
    val line = "askldjlkasdjklasdj"
    
    val error = CompilationError(line, "foo.loam", 5, message)
    
    val newLine = System.lineSeparator
    
    val expected = s"foo.loam: 'asldjaslkdjlkasdj'${newLine}askldjlkasdjklasdj${newLine}     ^"
    
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

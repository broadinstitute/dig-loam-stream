package loamstream.compiler

/**
 * @author clint
 * Aug 15, 2017
 */
sealed trait CompilationError {
  def toHumanReadableString: String
}

object CompilationError {
  final case class HasPosition(
      line: String, 
      sourceFileName: String, 
      column: Int, 
      message: String) extends CompilationError {
    
    override def toHumanReadableString: String = {
      s"${sourceFileName}: '$message'${newLine}${line}${newLine}${" " * column}^"
    }
  }
  
  final case class NoPosition(message: String) extends CompilationError {
    override def toHumanReadableString: String = s"(unknown source file): '$message'"
  }
  
  private val newLine = System.lineSeparator
  
  private[compiler] def hasSourceAndPosition(issue: Issue) = issue.pos.isDefined && issue.pos.source.content.nonEmpty
  
  def from(issue: Issue): CompilationError = {
    if(hasSourceAndPosition(issue)) {
      val line = issue.pos.lineContent
      
      val column = issue.pos.column
      
      val sourceFile = issue.pos.source.path.replace(".scala", ".loam")
      
      HasPosition(line, sourceFile, column - 1, issue.msg)
    } else {
      NoPosition(issue.msg)
    }
  }
}

package loamstream.compiler

/**
 * @author clint
 * Aug 15, 2017
 */
final case class CompilationError(line: String, sourceFileName: String, column: Int, message: String) {
  import CompilationError.newLine
  
  def toHumanReadableString: String = s"${sourceFileName}: '$message'${newLine}${line}${newLine}${" " * column}^"
}

object CompilationError {
  private val newLine = System.lineSeparator
  
  private[compiler] def hasSourcePosition(issue: Issue) = issue.pos.isDefined && issue.pos.source.content.nonEmpty
  
  def from(issue: Issue): Option[CompilationError] = {
    if(hasSourcePosition(issue)) {
      val line = issue.pos.lineContent
      
      val column = issue.pos.column
      
      val sourceFile = issue.pos.source.path.replace(".scala", ".loam")
      
      Some(CompilationError(line, sourceFile, column - 1, issue.msg))
    } else {
      None
    }
  }
}

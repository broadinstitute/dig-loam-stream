package loamstream.compiler.messages

import loamstream.compiler.Issue

/** A message that the compiler has found an issue */
case class CompilerIssueMessage(issue: Issue)
  extends ClientOutMessage {
  override val typeName = "compiler"

  override def message: String = issue.summary
}

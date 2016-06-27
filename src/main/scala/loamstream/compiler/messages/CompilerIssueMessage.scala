package loamstream.compiler.messages

import loamstream.compiler.Issue

/**
  * LoamStream
  * Created by oliverr on 5/11/2016.
  */
case class CompilerIssueMessage(issue: Issue)
  extends ClientOutMessage {
  override val typeName = "compiler"

  override def message: String = issue.summary
}

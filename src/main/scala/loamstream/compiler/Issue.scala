package loamstream.compiler

import loamstream.compiler.Issue.Severity

import scala.reflect.internal.util.Position

/**
  * LoamStream
  * Created by oliverr on 5/20/2016.
  */
object Issue {

  object Severity {
    def apply(level: Int): Severity = level match {
      case 0 => Info
      case 1 => Warning
      case 2 => Error
    }
  }

  sealed trait Severity {
    def level: Int

    def isProblem: Boolean
  }

  case object Info extends Severity {
    override val level = 0

    override def isProblem: Boolean = false
  }

  case object Warning extends Severity {
    override val level = 1

    override def isProblem: Boolean = true
  }

  case object Error extends Severity {
    override val level = 2

    override def isProblem: Boolean = true
  }


}

case class Issue(pos: Position, msg: String, severity: Severity) {
  def summary: String = s"[$severity] $msg"
}


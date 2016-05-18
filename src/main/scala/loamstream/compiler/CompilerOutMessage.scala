package loamstream.compiler

import loamstream.compiler.CompilerOutMessage.Severity

import scala.reflect.internal.util.Position

/**
  * LoamStream
  * Created by oliverr on 5/11/2016.
  */
object CompilerOutMessage {

  object Severity {
    def apply(level: Int): Severity = level match {
      case 0 => Info
      case 1 => Warning
      case 2 => Error
    }
  }

  sealed trait Severity {
    def level: Int
  }

  case object Info extends Severity {
    override val level = 0
  }

  case object Warning extends Severity {
    override val level = 1
  }

  case object Error extends Severity {
    override val level = 2
  }

}

case class CompilerOutMessage(pos: Position, message: String, severity: Severity, force: Boolean)
  extends ClientOutMessage {
  override val typeName = "compiler"
}

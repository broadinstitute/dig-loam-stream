package loamstream.compiler

import loamstream.compiler.Issue.Severity

import scala.reflect.internal.util.Position

/**
  * LoamStream
  * Created by oliverr on 5/20/2016.
  */
final case class Issue(pos: Position, msg: String, severity: Severity) {
  //NB: Include the position to get at least a little help figuring out where compile errors come from. 
  def summary: String = s"[$severity] $pos $msg"
}

object Issue {

  sealed trait Severity {
    def level: Int

    def isProblem: Boolean
  }
  
  object Severity {
    def apply(level: Int): Severity = {
      require(level == Info.level || level == Warning.level || level == Error.level)
      
      level match {
        case Info.level => Info
        case Warning.level => Warning
        case Error.level => Error
      }
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
}

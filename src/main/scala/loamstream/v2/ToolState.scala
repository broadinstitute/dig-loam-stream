package loamstream.v2

sealed trait ToolState {
  import ToolState._
  
  final def isNotStarted: Boolean = this == NotStarted
  final def isRunning: Boolean = this == Running
  final def isFinished: Boolean = this == Finished
  final def isFailed: Boolean = this == Failed
  
  final def isTerminal: Boolean = isFinished || isFailed
}

object ToolState {
  final case object NotStarted extends ToolState
  final case object Running extends ToolState
  final case object Finished extends ToolState
  final case object Failed extends ToolState
}

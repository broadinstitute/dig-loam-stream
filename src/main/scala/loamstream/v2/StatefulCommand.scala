package loamstream.v2

final case class StatefulCommand(state: ToolState, command: Command)

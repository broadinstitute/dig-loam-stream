package loamstream.v2

object Helpers {
  def toolStateString(state: EvaluationState): String = {
    state.toolStates.map { case (t, ts) => s"${t.id} => $ts" }.mkString("\nToolStates(\n\t", "\n\t", "\n)")
  }
}

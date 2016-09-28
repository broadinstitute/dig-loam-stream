package loamstream.model.jobs

/**
 * @author clint
 * date: Sep 22, 2016
 */
final case class Execution(exitStatus: Int, outputs: Set[Output]) {
  def transformOutputs(f: Set[Output] => Set[Output]): Execution = copy(outputs = f(outputs))
}
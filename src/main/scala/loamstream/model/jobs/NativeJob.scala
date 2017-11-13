package loamstream.model.jobs

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import loamstream.model.execute.Environment
import loamstream.util.EvalLaterBox


/** Job defined by Loam code */
final case class NativeJob[T](
    exprBox: EvalLaterBox[T], 
    inputs: Set[LJob] = Set.empty,
    outputs: Set[Output] = Set.empty,
    private val nameOpt: Option[String] = None) extends LJob {
  
  override def name: String = nameOpt.getOrElse(id)
  
  override def toString: String = s"${getClass.getSimpleName}#${id}(?,?,?)" 
  
  override def executionEnvironment: Environment = Environment.Local
  
  override protected def doWithInputs(newInputs: Set[LJob]): LJob = copy(inputs = newInputs)
}

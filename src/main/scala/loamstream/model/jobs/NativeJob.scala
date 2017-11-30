package loamstream.model.jobs



import loamstream.model.execute.Environment
import loamstream.util.EvalLaterBox


/** Job defined by Loam code */
final case class NativeJob[T](
    exprBox: EvalLaterBox[T], 
    override val inputs: Set[JobNode] = Set.empty,
    outputs: Set[Output] = Set.empty,
    private val nameOpt: Option[String] = None) extends LJob {
  
  override def name: String = nameOpt.getOrElse(id)
  
  override def toString: String = s"${getClass.getSimpleName}#${id}(?,?,?)" 
  
  override def executionEnvironment: Environment = Environment.Local
}

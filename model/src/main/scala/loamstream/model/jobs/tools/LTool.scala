package loamstream.model.jobs.tools

import loamstream.model.jobs.LJob
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
trait LTool[T] {

  def toolKind: LKind

  def outputKind: LKind

  def createJob(inputTools: Seq[LTool[_]]): LJob[T]

}

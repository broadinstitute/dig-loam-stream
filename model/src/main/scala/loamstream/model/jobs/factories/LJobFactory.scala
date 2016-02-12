package loamstream.model.jobs.factories

import loamstream.model.jobs.LJob
import loamstream.model.kinds.LKind

/**
  * LoamStream
  * Created by oliverr on 2/12/2016.
  */
trait LJobFactory[T] {

  def outputKind: LKind

  def createJob(inputJobFactories: Seq[LJobFactory[_]]): LJob[T]

}

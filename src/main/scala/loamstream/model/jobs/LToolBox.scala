package loamstream.model.jobs

import loamstream.model.execute.LExecutable
import loamstream.model.{AST, LPipeline, Tool}
import loamstream.util.{Hit, Shot}

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
trait LToolBox {
  def toolToJobShot(tool: Tool): Shot[LJob]

  def createExecutable(ast: AST): LExecutable = {
    val noJobs: Set[LJob] = Set.empty

    val jobs: Set[LJob] = ast match {
      case AST.ToolNode(id, tool, deps) =>
        val jobsOption = for {
        //TODO: Don't convert to option, pass misses through and fail loudly
          job <- toolToJobShot(tool).asOpt
          newInputs = deps.map(_.producer).flatMap(createExecutable(_).jobs)
          newJob = if(newInputs == job.inputs) job else job.withInputs(newInputs)
        } yield {
          Set[LJob](newJob)
        }

        jobsOption.getOrElse(noJobs)
      case _ => noJobs //TODO: other AST nodes
    }

    LExecutable(jobs)
  }

  def ++(oBox: LToolBox): LComboToolBox = oBox match {
    case LComboToolBox(boxes) => LComboToolBox(boxes + this)
    case _ => LToolBox(this, oBox)
  }
}

object LToolBox {
  def apply(box: LToolBox, boxes: LToolBox*): LComboToolBox = LComboToolBox(boxes.toSet + box)
}
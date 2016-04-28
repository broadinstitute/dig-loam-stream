package loamstream.apps.minimal

import loamstream.LEnv
import loamstream.model.LPipeline
import loamstream.model.execute.LExecutable
import loamstream.model.jobs.{LJob, LToolBox}
import loamstream.tools.core.LCoreDefaultStoreIds
import loamstream.tools.core.LCoreEnv
import loamstream.util.{Hit, Miss, Shot}
import loamstream.util.SnagMessage
import loamstream.model.Store
import loamstream.model.Tool


/**
  * LoamStream
  * Created by oliverr on 3/31/2016.
  */
object MiniMockToolBox {
  def apply(env: LEnv): Shot[MiniMockToolBox] = {
    env.get(LCoreEnv.Keys.genotypesId) match {
      case Some(genotypesId) => Hit(MiniMockToolBox(genotypesId))
      case None => Miss("Genotypes id not defined in environment.")
    }
  }
}

case class MiniMockToolBox(genotypesId: String = LCoreDefaultStoreIds.genotypes) extends LToolBox {
  val stores: Set[Store] = MiniMockStore.stores
  val tools: Set[Tool] = MiniMockTool.tools(genotypesId)

  override def createJobs(tool: Tool, pipeline: LPipeline): Shot[Set[LJob]] = {
    Miss(SnagMessage(s"Have not yet implemented tool $tool"))
  }

  override def createExecutable(pipeline: LPipeline): LExecutable = {
    LExecutable(pipeline.tools.map(createJobs(_, pipeline)).collect { case Hit(job) => job }.flatten)
  }
}

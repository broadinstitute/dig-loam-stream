package loamstream.compiler.v2

import loamstream.loam.LoamGraph
import loamstream.compiler.LoamProject
import loamstream.loam.LoamProjectContext
import loamstream.util.ValueBox
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamSyntax

abstract class LoamFile {
  protected lazy val loam: LoamSyntax = LoamSyntax 
  
  protected implicit lazy val scriptContext: LoamScriptContext = new LoamScriptContext(LoamFile.projectContext)
  
  protected def graph: LoamGraph = scriptContext.projectContext.graph
  
  protected def logFooter(): Unit = {
    println(s"######### Evaluated ${this.getClass.getName} !!")
    println(s"######### Graph now has ${graph.tools.size} tools !!")
  }
}

object LoamFile {
  private[v2] def config: LoamConfig = configBox.value.get
  private[v2] def config_=(newConfig: LoamConfig): Unit = configBox.update(Option(newConfig))
  private[this] val configBox: ValueBox[Option[LoamConfig]] = ValueBox(None)
  
  private[v2] def clearGraph(): Unit = projectContext.updateGraph(_ => LoamGraph.empty)
  
  private[v2] lazy val projectContext: LoamProjectContext = LoamProjectContext.empty(config)
  
  private[v2] def graph: LoamGraph = projectContext.graph
}

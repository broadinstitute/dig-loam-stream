package loamstream.loam

import loamstream.model.Tool
import loamstream.conf.LoamConfig
import com.typesafe.config.ConfigFactory

object Foo extends App {
  def anyCycles(graph: LoamGraph): Boolean = {
    def anyCyclesStartingFrom(t: Tool): Boolean = {
      def loop(seen: Set[Tool], preceding: Set[Tool]): Boolean = {
        if(preceding.isEmpty) { false }
        else if(preceding.exists(seen.contains(_))) { true }
        else { loop(seen ++ preceding, preceding.flatMap(graph.toolsPreceding)) }
      }
      
      loop(Set(t), graph.toolsPreceding(t))
    }
    
    graph.tools.exists(anyCyclesStartingFrom)
  }
  
  def findCycles(graph: LoamGraph): Set[Seq[Tool]] = {
    def findCycleStartingFrom(t: Tool): Option[Seq[Tool]] = {
      def walkFrom(current: Tool, soFar: Seq[Tool], seen: Set[Tool]): Option[Seq[Tool]] = {
        val preceding = graph.toolsPreceding(current)
        
        if(preceding.isEmpty) { None }
        else if(preceding.exists(seen.contains(_))) { 
          preceding.collectFirst { case p if seen.contains(p) => p }.map(offending => (offending +: soFar)) 
        }
        else { 
          val cycles = preceding.iterator.map(p => walkFrom(p, p +: soFar, seen + p)).filter(_.isDefined)
              
          cycles.toStream.headOption.flatten
        }
      }
      
      walkFrom(t, Seq(t), Set(t))
    }
    
    graph.tools.map(findCycleStartingFrom(_).toSet).flatten
  }
  
  def pathToString(graph: LoamGraph)(path: Seq[Tool]): String = {
    path.map(t => s"'${graph.nameOf(t).get}'").mkString(" => ")
  }
  
  val graphWithCycle: LoamGraph = {
    val config = LoamConfig.fromConfig(ConfigFactory.empty).get
    
    implicit val context: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))
    
    import LoamSyntax._
    
    val storeA = store("a.txt").asInput
    val storeB = store("b.txt").asInput
    val storeC = store("c.txt").asInput
    
    cmd"a2b".tag("a2b").in(storeA).out(storeB)
    cmd"b2c".tag("b2c").in(storeB).out(storeC)
    cmd"c2a".tag("c2a").in(storeC).out(storeA)
    
    context.projectContext.graph
  }
  
  println(anyCycles(graphWithCycle))
  findCycles(graphWithCycle).map(pathToString(graphWithCycle)).foreach(println)
  
  val graphWITHOUTCycle: LoamGraph = {
    val config = LoamConfig.fromConfig(ConfigFactory.empty).get
    
    implicit val context: LoamScriptContext = new LoamScriptContext(LoamProjectContext.empty(config))
    
    import LoamSyntax._
    
    val storeA = store("a.txt").asInput
    val storeB = store("b.txt").asInput
    val storeC = store("c.txt").asInput
    
    cmd"a2b".tag("a2b").in(storeA).out(storeB)
    cmd"b2c".tag("b2c").in(storeB).out(storeC)
    
    context.projectContext.graph
  }
  
  println(anyCycles(graphWITHOUTCycle))
  findCycles(graphWITHOUTCycle).map(pathToString(graphWITHOUTCycle)).foreach(println)
}

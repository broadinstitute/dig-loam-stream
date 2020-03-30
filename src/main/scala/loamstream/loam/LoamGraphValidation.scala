package loamstream.loam

import loamstream.model.{Store, Tool}
import loamstream.util.Validation
import loamstream.util.Validation.{BulkIssue, BulkValidation, Issue, Severity, SimpleIssue}
import loamstream.util.Sets

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object LoamGraphValidation {

  type LoamBulkRule[Target, Details] = BulkValidation[LoamGraph, Target, Details]
  type LoamIssue[Details] = Issue[LoamGraph, Details]
  type LoamBulkIssue[Target, Details] = BulkIssue[LoamGraph, Target, Details]
  type LoamStoreIssue[Details] = LoamBulkIssue[Store, Details]
  type LoamToolIssue[Details] = LoamBulkIssue[Tool, Details]
  type LoamProducerIssue[Details] = LoamBulkIssue[(Store, Tool), Details]
  type LoamConsumerIssue[Details] = LoamBulkIssue[(Store, Tool), Details]

  def newIssue[Details](graph: LoamGraph, rule: LoamGlobalRule[Details], details: Details, severity: Severity,
                        message: String):
  LoamIssue[Details] =
    SimpleIssue[LoamGraph, Details](graph, rule, details, severity, message)

  def newBulkIssue[Target, Details](graph: LoamGraph, rule: LoamBulkRule[Target, Details], target: Target,
                                    details: Details, severity: Severity, message: String):
  LoamBulkIssue[Target, Details] =
    BulkIssue[LoamGraph, Target, Details](graph, rule, target, details, severity, message)

  def issueIf[I <: LoamIssue[_]](cond: Boolean, issue: I): Seq[I] = if (cond) Seq(issue) else Seq.empty

  def issueIfElseIf[I <: LoamBulkIssue[_, _]](cond1: Boolean, issue1: I, cond2: Boolean, issue2: I): Seq[I] =
    if (cond1) Seq(issue1) else if (cond2) Seq(issue2) else Seq.empty

  trait LoamGlobalRule[Details] extends Validation[LoamGraph]

  trait LoamStoreRule[Details] extends LoamBulkRule[Store, Details] {
    override def targets(graph: LoamGraph): Seq[Store] = graph.stores.toSeq
  }

  trait LoamToolRule[Details] extends LoamBulkRule[Tool, Details] {
    override def targets(graph: LoamGraph): Seq[Tool] = graph.tools.toSeq
  }

  trait LoamProducerRule[Details] extends LoamBulkRule[(Store, Tool), Details] {
    override def targets(graph: LoamGraph): Seq[(Store, Tool)] =
      graph.stores.flatMap(store =>
        graph.storeProducers.get(store).map(tool => (store, tool))).toSeq
  }

  trait LoamConsumerRule[Details] extends LoamBulkRule[(Store, Tool), Details] {
    override def targets(graph: LoamGraph): Seq[(Store, Tool)] =
      graph.stores.flatMap(store =>
        graph.storeConsumers.getOrElse(store, Set.empty).map(tool => (store, tool))).toSeq
  }

  object eachStoreIsInputOrHasProducer extends LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: Store): Seq[LoamStoreIssue[Unit]] =
      issueIfElseIf(!graph.inputStores(store) && graph.storeProducers.get(store).isEmpty,
        newBulkIssue[Store, Unit](graph, this, store, (), Severity.Warning,
          s"Store $store is neither input store nor has a producer"),
        graph.inputStores(store) && graph.storeProducers.get(store).nonEmpty,
        newBulkIssue[Store, Unit](graph, this, store, (), Severity.Warning,
          s"Store $store is both input store and has a producer"))
  }

  object eachStoresIsOutputOfItsProducer extends LoamProducerRule[Unit] {
    override def apply(graph: LoamGraph, producerEntry: (Store, Tool)): Seq[LoamProducerIssue[Unit]] = {
      val (store, tool) = producerEntry
      issueIf(!graph.toolOutputs.getOrElse(tool, Set.empty).contains(store),
        newBulkIssue[(Store, Tool), Unit](graph, this, producerEntry, (), Severity.Warning,
          s"Tool $tool is producer of store $store, but the store is not among its outputs."))
    }
  }

  object eachStoresIsInputOfItsConsumers extends LoamConsumerRule[Unit] {
    override def apply(graph: LoamGraph, consumerEntry: (Store, Tool)): Seq[LoamConsumerIssue[Unit]] = {
      val (store, tool) = consumerEntry
      issueIf(!graph.toolInputs.getOrElse(tool, Set.empty).contains(store),
        newBulkIssue[(Store, Tool), Unit](graph, this, consumerEntry, (), Severity.Warning,
          s"Tool $tool is consumer of store $store, but the store is not among its inputs."))
    }
  }
  
  object eachToolsInputStoresArePresent extends LoamToolRule[Set[Store]] {
    override def apply(graph: LoamGraph, tool: Tool): Seq[LoamToolIssue[Set[Store]]] = {
      //NB: Use hashSetDiff because `--` had O(n^2) running time :( 
      val missingInputs = Sets.hashSetDiff(graph.toolInputs.getOrElse(tool, Set.empty), graph.stores)
      
      issueIf(missingInputs.nonEmpty,
        newBulkIssue[Tool, Set[Store]](graph, this, tool, missingInputs, Severity.Warning,
          s"The following inputs of tool $tool are missing from the graph: ${missingInputs.mkString(", ")}"))
    }
  }

  object eachToolsOutputStoresArePresent extends LoamToolRule[Set[Store]] {
    override def apply(graph: LoamGraph, tool: Tool): Seq[LoamToolIssue[Set[Store]]] = {
      //NB: Use hashSetDiff because `--` had O(n^2) running time :(
      val missingOutputs = Sets.hashSetDiff(graph.toolOutputs.getOrElse(tool, Set.empty), graph.stores)
      
      issueIf(missingOutputs.nonEmpty,
        newBulkIssue[Tool, Set[Store]](graph, this, tool, missingOutputs, Severity.Warning,
          s"The following outputs of tool $tool are missing from the graph: ${missingOutputs.mkString(", ")}"))
    }
  }

  object noToolsPrecedeInitialTool extends LoamToolRule[Set[Tool]] {
    override def targets(graph: LoamGraph): Seq[Tool] = graph.initialTools.toSeq

    override def apply(graph: LoamGraph, tool: Tool): Seq[BulkIssue[LoamGraph, Tool, Set[Tool]]] = {
      val precedingTools = graph.toolInputs.getOrElse(tool, Set.empty).flatMap(graph.storeProducers.get)
      issueIf(precedingTools.nonEmpty,
        newBulkIssue[Tool, Set[Tool]](graph, this, tool, precedingTools, Severity.Warning,
          s"Tool $tool is considered initial, but the following tools precede it: ${precedingTools.mkString(", ")}.")
      )
    }
  }

  object noToolsSucceedFinalTool extends LoamToolRule[Set[Tool]] {
    override def targets(graph: LoamGraph): Seq[Tool] = graph.finalTools.toSeq

    override def apply(graph: LoamGraph, tool: Tool): Seq[BulkIssue[LoamGraph, Tool, Set[Tool]]] = {
      val succeedingTools =
        graph.toolOutputs.getOrElse(tool, Set.empty).flatMap(graph.storeConsumers.getOrElse(_, Set.empty))
      issueIf(succeedingTools.nonEmpty,
        newBulkIssue[Tool, Set[Tool]](graph, this, tool, succeedingTools, Severity.Warning,
          s"Tool $tool is considered final, but the following tools succeed it: ${succeedingTools.mkString(", ")}.")
      )
    }
  }

  object eachStoreIsConnectedToATool extends LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: Store): Seq[LoamStoreIssue[Unit]] = {
      val storeIsInput = graph.storeConsumers.getOrElse(store, Set.empty).nonEmpty
      val storeIsOutput = graph.storeProducers.contains(store)
      issueIf(!(storeIsInput || storeIsOutput),
        newBulkIssue[Store, Unit](graph, this, store, (), Severity.Warning,
          s"Store $store is neither input nor output of any tool."))
    }
  }

  object eachToolHasEitherInputsOrOutputs extends LoamToolRule[Unit] {
    override def apply(graph: LoamGraph, tool: Tool): Seq[LoamToolIssue[Unit]] = {
      val hasNoInputs = graph.toolInputs.getOrElse(tool, Set.empty).isEmpty
      val hasNoOutputs = graph.toolOutputs.getOrElse(tool, Set.empty).isEmpty
      issueIf(hasNoInputs && hasNoOutputs,
        newBulkIssue[Tool, Unit](graph, this, tool, (), Severity.Warning,
          s"Tool $tool has neither inputs nor outputs."))
    }
  }

  object allToolsAreConnected extends LoamGlobalRule[(Tool, Tool)] {
    def nearestNeighbours(graph: LoamGraph, tool: Tool): Set[Tool] = {
      graph.toolsPreceding(tool) ++ graph.toolsSucceeding(tool)
    }

    override def apply(graph: LoamGraph): Seq[LoamIssue[(Tool, Tool)]] = {
      if (graph.tools.size > 1) {
        val tool1 = graph.tools.head
        var connectedTools = Set(tool1)
        var front = connectedTools
        while (front.nonEmpty) {
          connectedTools ++= front
          front = front.flatMap(nearestNeighbours(graph, _)) -- connectedTools
        }
        val disconnectedTools = graph.tools -- connectedTools
        if (disconnectedTools.nonEmpty) {
          val tool2 = disconnectedTools.head
          val issue =
            newIssue[(Tool, Tool)](graph, this, (tool1, tool2), Severity.Warning,
              s"Tools $tool1 and $tool2 are not connected.")
          Seq(issue)
        } else {
          Seq.empty
        }
      } else {
        Seq.empty
      }
    }
  }

  object graphIsAcyclic extends LoamGlobalRule[Set[Tool]] {
    override def apply(graph: LoamGraph): Seq[LoamIssue[Set[Tool]]] = {
      var tools = graph.tools
      var makingProgress = true
      while (tools.nonEmpty && makingProgress) {
        val toolsNew = {
          tools.intersect(tools.flatMap(graph.toolsPreceding)).intersect(tools.flatMap(graph.toolsSucceeding))
        }
        makingProgress = toolsNew.size < tools.size
        tools = toolsNew
      }
      issueIf(tools.nonEmpty,
        newIssue[Set[Tool]](graph, this, tools, Severity.Warning,
          s"Graph contains one or more cycles including tools ${tools.mkString(", ")}"))
    }
  }
  
  val consistencyRules: Validation.Composite[LoamGraph] = {
    eachStoreIsInputOrHasProducer ++ eachStoresIsOutputOfItsProducer ++ eachStoresIsInputOfItsConsumers ++
      eachToolsInputStoresArePresent ++ eachToolsOutputStoresArePresent ++ noToolsPrecedeInitialTool ++
      noToolsSucceedFinalTool
  }
  
  val connectivityRules: Validation.Composite[LoamGraph] = {
    eachStoreIsConnectedToATool ++ eachToolHasEitherInputsOrOutputs ++ allToolsAreConnected
  }
  
  val allRules: Validation.Composite[LoamGraph] = consistencyRules ++ connectivityRules ++ graphIsAcyclic
}

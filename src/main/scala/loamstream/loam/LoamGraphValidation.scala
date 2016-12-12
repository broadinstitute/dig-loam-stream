package loamstream.loam

import loamstream.loam.LoamGraph.StoreEdge.ToolEdge
import loamstream.util.Validation
import loamstream.util.Validation.{BulkIssue, BulkValidation, Issue, Severity, SimpleIssue}

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object LoamGraphValidation {

  type LoamBulkRule[Target, Details] = BulkValidation[LoamGraph, Target, Details]
  type LoamIssue[Details] = Issue[LoamGraph, Details]
  type LoamBulkIssue[Target, Details] = BulkIssue[LoamGraph, Target, Details]
  type LoamStoreIssue[Details] = LoamBulkIssue[LoamStore.Untyped, Details]
  type LoamToolIssue[Details] = LoamBulkIssue[LoamTool, Details]
  type LoamSourceIssue[Details] = LoamBulkIssue[(LoamStore.Untyped, LoamGraph.StoreEdge), Details]
  type LoamConsumerIssue[Details] = LoamBulkIssue[(LoamStore.Untyped, LoamTool), Details]

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

  trait LoamStoreRule[Details] extends LoamBulkRule[LoamStore.Untyped, Details] {
    override def targets(graph: LoamGraph): Seq[LoamStore.Untyped] = graph.stores.toSeq
  }

  trait LoamToolRule[Details] extends LoamBulkRule[LoamTool, Details] {
    override def targets(graph: LoamGraph): Seq[LoamTool] = graph.tools.toSeq
  }

  trait LoamSourceRule[Details] extends LoamBulkRule[(LoamStore.Untyped, LoamGraph.StoreEdge), Details] {
    override def targets(graph: LoamGraph): Seq[(LoamStore.Untyped, LoamGraph.StoreEdge)] = graph.storeSources.toSeq
  }

  trait LoamConsumerRule[Details] extends LoamBulkRule[(LoamStore.Untyped, LoamTool), Details] {
    override def targets(graph: LoamGraph): Seq[(LoamStore.Untyped, LoamTool)] =
      graph.stores.flatMap(store =>
        graph.storeSinks.getOrElse(store, Set.empty).collect({ case ToolEdge(tool) => (store, tool) })).toSeq
  }

  val eachStoreHasASource: LoamStoreRule[Unit] = new LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: LoamStore.Untyped): Seq[LoamStoreIssue[Unit]] =
      issueIf(graph.storeSources.get(store).isEmpty,
        newBulkIssue[LoamStore.Untyped, Unit](graph, this, store, (), Severity.Error, s"No source for store $store"))
  }

  val eachToolSourcedStoreIsOutputOfThatTool: LoamSourceRule[Unit] = new LoamSourceRule[Unit] {
    override def apply(graph: LoamGraph, sourceEntry: (LoamStore.Untyped, LoamGraph.StoreEdge)):
    Seq[LoamSourceIssue[Unit]] = sourceEntry match {
      case (store, fromTool: LoamGraph.StoreEdge.ToolEdge) =>
        val tool = fromTool.tool
        issueIfElseIf(!graph.tools(tool),
          newBulkIssue[(LoamStore.Untyped, LoamGraph.StoreEdge), Unit](graph, this, (store, fromTool), (),
            Severity.Error, s"Store $store has as source tool $tool, but this tool is not part of the graph."),
          !graph.toolOutputs.getOrElse(tool, Set.empty).contains(store),
          newBulkIssue[(LoamStore.Untyped, LoamGraph.StoreEdge), Unit](graph, this, (store, fromTool), (),
            Severity.Error, s"Store $store has as source tool $tool, but is not an output of that tool."))
      case _ => Seq.empty
    }
  }

  val eachStoresIsInputOfItsConsumers: LoamConsumerRule[Unit] = new LoamConsumerRule[Unit] {
    override def apply(graph: LoamGraph, consumerEntry: (LoamStore.Untyped, LoamTool)):
    Seq[LoamConsumerIssue[Unit]] = {
      val (store, tool) = consumerEntry
      issueIf(!graph.toolInputs.getOrElse(tool, Set.empty).contains(store),
        newBulkIssue[(LoamStore.Untyped, LoamTool), Unit](graph, this, consumerEntry, (), Severity.Error,
          s"Tool $tool is consumer of store $store, but the store is not among its inputs."))
    }
  }

  val eachToolsInputStoresArePresent: LoamToolRule[Set[LoamStore.Untyped]] =
    new LoamToolRule[Set[LoamStore.Untyped]] {
    override def apply(graph: LoamGraph, tool: LoamTool): Seq[LoamToolIssue[Set[LoamStore.Untyped]]] = {
      val missingInputs = graph.toolInputs.getOrElse(tool, Set.empty) -- graph.stores
      issueIf(missingInputs.nonEmpty,
        newBulkIssue[LoamTool, Set[LoamStore.Untyped]](graph, this, tool, missingInputs, Severity.Error,
          s"The following inputs of tool $tool are missing from the graph: ${missingInputs.mkString(", ")}"))
    }
  }

  val eachToolsOutputStoresArePresent: LoamToolRule[Set[LoamStore.Untyped]] =
    new LoamToolRule[Set[LoamStore.Untyped]] {
    override def apply(graph: LoamGraph, tool: LoamTool): Seq[LoamToolIssue[Set[LoamStore.Untyped]]] = {
      val missingOutputs = graph.toolOutputs.getOrElse(tool, Set.empty) -- graph.stores
      issueIf(missingOutputs.nonEmpty,
        newBulkIssue[LoamTool, Set[LoamStore.Untyped]](graph, this, tool, missingOutputs, Severity.Error,
          s"The following outputs of tool $tool are missing from the graph: ${missingOutputs.mkString(", ")}"))
    }
  }

  val noToolsPrecedeInitialTool: LoamToolRule[Set[LoamTool]] = new LoamToolRule[Set[LoamTool]] {
    override def targets(graph: LoamGraph): Seq[LoamTool] = graph.initialTools.toSeq

    override def apply(graph: LoamGraph, tool: LoamTool): Seq[BulkIssue[LoamGraph, LoamTool, Set[LoamTool]]] = {
      val precedingTools = graph.toolInputs.getOrElse(tool, Set.empty).flatMap(graph.storeProducerOpt)
      issueIf(precedingTools.nonEmpty,
        newBulkIssue[LoamTool, Set[LoamTool]](graph, this, tool, precedingTools, Severity.Error,
          s"Tool $tool is considered initial, but the following tools precede it: ${precedingTools.mkString(", ")}.")
      )
    }
  }

  val noToolsSucceedFinalTool: LoamToolRule[Set[LoamTool]] = new LoamToolRule[Set[LoamTool]] {
    override def targets(graph: LoamGraph): Seq[LoamTool] = graph.finalTools.toSeq

    override def apply(graph: LoamGraph, tool: LoamTool): Seq[BulkIssue[LoamGraph, LoamTool, Set[LoamTool]]] = {
      val succeedingTools = graph.toolOutputs.getOrElse(tool, Set.empty).flatMap(graph.storeConsumers)
      issueIf(succeedingTools.nonEmpty,
        newBulkIssue[LoamTool, Set[LoamTool]](graph, this, tool, succeedingTools, Severity.Error,
          s"Tool $tool is considered final, but the following tools succeed it: ${succeedingTools.mkString(", ")}.")
      )
    }
  }

  val eachStoreIsConnectedToATool: LoamStoreRule[Unit] = new LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: LoamStore.Untyped): Seq[LoamStoreIssue[Unit]] = {
      val storeIsInput = graph.storeConsumers(store).nonEmpty
      val storeIsOutput = graph.storeProducerOpt(store).nonEmpty
      issueIf(!(storeIsInput || storeIsOutput),
        newBulkIssue[LoamStore.Untyped, Unit](graph, this, store, (), Severity.Error,
          s"Store $store is neither input nor output of any tool."))
    }
  }

  val eachToolHasEitherInputsOrOutputs: LoamToolRule[Unit] = new LoamToolRule[Unit] {
    override def apply(graph: LoamGraph, tool: LoamTool): Seq[LoamToolIssue[Unit]] = {
      val hasNoInputs = graph.toolInputs.getOrElse(tool, Set.empty).isEmpty
      val hasNoOutputs = graph.toolOutputs.getOrElse(tool, Set.empty).isEmpty
      issueIf(hasNoInputs && hasNoOutputs,
        newBulkIssue[LoamTool, Unit](graph, this, tool, (), Severity.Error,
          s"Tool $tool has neither inputs nor outputs."))
    }
  }

  val allToolsAreConnected: LoamGlobalRule[(LoamTool, LoamTool)] =
    new LoamGlobalRule[(LoamTool, LoamTool)] {
      def nearestNeighbours(graph: LoamGraph, tool: LoamTool): Set[LoamTool] =
        graph.toolsPreceding(tool) ++ graph.toolsSucceeding(tool)

      override def apply(graph: LoamGraph): Seq[LoamIssue[(LoamTool, LoamTool)]] = {
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
              newIssue[(LoamTool, LoamTool)](graph, this, (tool1, tool2), Severity.Error,
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

  val graphIsAcyclic: LoamGlobalRule[Set[LoamTool]] = new LoamGlobalRule[Set[LoamTool]] {
    override def apply(graph: LoamGraph): Seq[LoamIssue[Set[LoamTool]]] = {
      var tools = graph.tools
      var makingProgress = true
      while (tools.nonEmpty && makingProgress) {
        val toolsNew =
          tools.intersect(tools.flatMap(graph.toolsPreceding)).intersect(tools.flatMap(graph.toolsSucceeding))
        makingProgress = toolsNew.size < tools.size
        tools = toolsNew
      }
      issueIf(tools.nonEmpty,
        newIssue[Set[LoamTool]](graph, this, tools, Severity.Error,
          s"Graph contains one or more cycles including tools ${tools.mkString(", ")}"))
    }
  }

  val consistencyRules = eachStoreHasASource ++ eachToolSourcedStoreIsOutputOfThatTool ++
    eachStoresIsInputOfItsConsumers ++ eachToolsInputStoresArePresent ++ eachToolsOutputStoresArePresent ++
    noToolsPrecedeInitialTool ++ noToolsSucceedFinalTool
  val connectivityRules = eachStoreIsConnectedToATool ++ eachToolHasEitherInputsOrOutputs ++ allToolsAreConnected
  val allRules = consistencyRules ++ connectivityRules ++ graphIsAcyclic

}

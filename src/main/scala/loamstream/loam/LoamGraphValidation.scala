package loamstream.loam

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
  type LoamStoreIssue[Details] = LoamBulkIssue[StoreBuilder, Details]
  type LoamToolIssue[Details] = LoamBulkIssue[ToolBuilder, Details]
  type LoamSourceIssue[Details] = LoamBulkIssue[(StoreBuilder, LoamGraph.StoreSource), Details]
  type LoamConsumerIssue[Details] = LoamBulkIssue[(StoreBuilder, ToolBuilder), Details]

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

  trait LoamStoreRule[Details] extends LoamBulkRule[StoreBuilder, Details] {
    override def targets(graph: LoamGraph): Seq[StoreBuilder] = graph.stores.toSeq
  }

  trait LoamToolRule[Details] extends LoamBulkRule[ToolBuilder, Details] {
    override def targets(graph: LoamGraph): Seq[ToolBuilder] = graph.tools.toSeq
  }

  trait LoamSourceRule[Details] extends LoamBulkRule[(StoreBuilder, LoamGraph.StoreSource), Details] {
    override def targets(graph: LoamGraph): Seq[(StoreBuilder, LoamGraph.StoreSource)] = graph.storeSources.toSeq
  }

  trait LoamConsumerRule[Details] extends LoamBulkRule[(StoreBuilder, ToolBuilder), Details] {
    override def targets(graph: LoamGraph): Seq[(StoreBuilder, ToolBuilder)] =
      graph.storeConsumers.flatMap({ case (store, tools) => tools.map((store, _)) }).toSeq
  }

  val eachStoreHasASource: LoamStoreRule[Unit] = new LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: StoreBuilder): Seq[LoamStoreIssue[Unit]] =
      issueIf(graph.storeSources.get(store).isEmpty,
        newBulkIssue[StoreBuilder, Unit](graph, this, store, (), Severity.Error, s"No source for store $store"))
  }

  val eachToolSourcedStoreIsOutputOfThatTool: LoamSourceRule[Unit] = new LoamSourceRule[Unit] {
    override def apply(graph: LoamGraph, sourceEntry: (StoreBuilder, LoamGraph.StoreSource)):
    Seq[LoamSourceIssue[Unit]] = sourceEntry match {
      case (store, fromTool: LoamGraph.StoreSource.FromTool) =>
        val tool = fromTool.tool
        issueIfElseIf(!graph.tools(tool),
          newBulkIssue[(StoreBuilder, LoamGraph.StoreSource), Unit](graph, this, (store, fromTool), (), Severity.Error,
            s"Store $store has as source tool $tool, but this tool is not part of the graph."),
          !graph.toolOutputs.getOrElse(tool, Set.empty).contains(store),
          newBulkIssue[(StoreBuilder, LoamGraph.StoreSource), Unit](graph, this, (store, fromTool), (), Severity.Error,
            s"Store $store has as source tool $tool, but is not an output of that tool."))
      case _ => Seq.empty
    }
  }

  val eachStoresIsInputOfItsConsumers: LoamConsumerRule[Unit] = new LoamConsumerRule[Unit] {
    override def apply(graph: LoamGraph, consumerEntry: (StoreBuilder, ToolBuilder)):
    Seq[LoamConsumerIssue[Unit]] = {
      val (store, tool) = consumerEntry
      issueIf(!graph.toolInputs.getOrElse(tool, Set.empty).contains(store),
        newBulkIssue[(StoreBuilder, ToolBuilder), Unit](graph, this, consumerEntry, (), Severity.Error,
          s"Tool $tool is consumer of store $store, but the store is not among its inputs."))
    }
  }

  val eachToolsInputStoresArePresent: LoamToolRule[Set[StoreBuilder]] = new LoamToolRule[Set[StoreBuilder]] {
    override def apply(graph: LoamGraph, tool: ToolBuilder): Seq[LoamToolIssue[Set[StoreBuilder]]] = {
      val missingInputs = graph.toolInputs.getOrElse(tool, Set.empty) -- graph.stores
      issueIf(missingInputs.nonEmpty,
        newBulkIssue[ToolBuilder, Set[StoreBuilder]](graph, this, tool, missingInputs, Severity.Error,
          s"The following inputs of tool $tool are missing from the graph: ${missingInputs.mkString(", ")}"))
    }
  }

  val eachToolsOutputStoresArePresent: LoamToolRule[Set[StoreBuilder]] = new LoamToolRule[Set[StoreBuilder]] {
    override def apply(graph: LoamGraph, tool: ToolBuilder): Seq[LoamToolIssue[Set[StoreBuilder]]] = {
      val missingOutputs = graph.toolOutputs.getOrElse(tool, Set.empty) -- graph.stores
      issueIf(missingOutputs.nonEmpty,
        newBulkIssue[ToolBuilder, Set[StoreBuilder]](graph, this, tool, missingOutputs, Severity.Error,
          s"The following outputs of tool $tool are missing from the graph: ${missingOutputs.mkString(", ")}"))
    }
  }

  val eachStoreIsConnectedToATool: LoamStoreRule[Unit] = new LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: StoreBuilder): Seq[LoamStoreIssue[Unit]] = {
      val storeIsInput = graph.storeConsumers.contains(store)
      val storeIsOutput = graph.storeSources.get(store) match {
        case Some(fromTool: LoamGraph.StoreSource.FromTool) => true
        case _ => false
      }
      issueIf(!(storeIsInput || storeIsOutput),
        newBulkIssue[StoreBuilder, Unit](graph, this, store, (), Severity.Error,
          s"Store $store is neither input nor output of any tool."))
    }
  }

  val eachToolHasEitherInputsOrOutputs: LoamToolRule[Unit] = new LoamToolRule[Unit] {
    override def apply(graph: LoamGraph, tool: ToolBuilder): Seq[LoamToolIssue[Unit]] = {
      val hasNoInputs = graph.toolInputs.getOrElse(tool, Set.empty).isEmpty
      val hasNoOutputs = graph.toolOutputs.getOrElse(tool, Set.empty).isEmpty
      issueIf(hasNoInputs && hasNoOutputs,
        newBulkIssue[ToolBuilder, Unit](graph, this, tool, (), Severity.Error,
          s"Tool $tool has neither inputs nor outputs."))
    }
  }

  val allToolsAreConnected: LoamGlobalRule[(ToolBuilder, ToolBuilder)] =
    new LoamGlobalRule[(ToolBuilder, ToolBuilder)] {
      def nearestNeighbours(graph: LoamGraph, tool: ToolBuilder): Set[ToolBuilder] =
        graph.toolsPreceding(tool) ++ graph.toolsSucceeding(tool)

      override def apply(graph: LoamGraph): Seq[LoamIssue[(ToolBuilder, ToolBuilder)]] = {
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
              newIssue[(ToolBuilder, ToolBuilder)](graph, this, (tool1, tool2), Severity.Error,
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

  val graphIsAcyclic: LoamGlobalRule[Set[ToolBuilder]] = new LoamGlobalRule[Set[ToolBuilder]] {
    override def apply(graph: LoamGraph): Seq[LoamIssue[Set[ToolBuilder]]] = {
      var tools = graph.tools
      var makingProgress = true
      while (tools.nonEmpty && makingProgress) {
        val toolsNew =
          tools.intersect(tools.flatMap(graph.toolsPreceding)).intersect(tools.flatMap(graph.toolsSucceeding))
        makingProgress = toolsNew.size < tools.size
        tools = toolsNew
      }
      issueIf(tools.nonEmpty,
        newIssue[Set[ToolBuilder]](graph, this, tools, Severity.Error,
          s"Graph contains one or more cycles including tools ${tools.mkString(", ")}"))
    }
  }

  val consistencyRules = eachStoreHasASource ++ eachToolSourcedStoreIsOutputOfThatTool ++
    eachStoresIsInputOfItsConsumers ++ eachToolsInputStoresArePresent ++ eachToolsOutputStoresArePresent
  val connectivityRules = eachStoreIsConnectedToATool ++ eachToolHasEitherInputsOrOutputs ++ allToolsAreConnected
  val allRules = consistencyRules ++ connectivityRules ++ graphIsAcyclic

}

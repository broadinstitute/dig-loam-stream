package loamstream.loam

import loamstream.util.Validator.{Issue, Rule, Severity}

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object LoamGraphValidation {

  type LoamRule[Target, Details] = Rule[LoamGraph, Target, Details]
  type LoamGlobalRule[Details] = Rule.SingleTarget[LoamGraph, Details]
  type LoamIssue[Target, Details] = Issue[LoamGraph, Target, Details]
  type LoamGlobalIssue[Details] = LoamIssue[LoamGraph, Details]
  type LoamStoreIssue[Details] = LoamIssue[StoreBuilder, Details]
  type LoamToolIssue[Details] = LoamIssue[ToolBuilder, Details]
  type LoamSourceIssue[Details] = LoamIssue[(StoreBuilder, LoamGraph.StoreSource), Details]
  type LoamConsumerIssue[Details] = LoamIssue[(StoreBuilder, ToolBuilder), Details]

  def newIssue[Target, Details](graph: LoamGraph, rule: LoamRule[Target, Details], target: Target, details: Details,
                                severity: Severity, message: String): LoamIssue[Target, Details] =
    Issue[LoamGraph, Target, Details](graph, rule, target, details, severity, message)

  def issueIf[I <: LoamIssue[_, _]](cond: Boolean, issue: I): Seq[I] = if (cond) Seq(issue) else Seq.empty

  def issueIfElseIf[I <: LoamIssue[_, _]](cond1: Boolean, issue1: I, cond2: Boolean, issue2: I): Seq[I] =
    if (cond1) Seq(issue1) else if (cond2) Seq(issue2) else Seq.empty

  trait LoamStoreRule[Details] extends LoamRule[StoreBuilder, Details] {
    override def targets(graph: LoamGraph): Seq[StoreBuilder] = graph.stores.toSeq
  }

  trait LoamToolRule[Details] extends LoamRule[ToolBuilder, Details] {
    override def targets(graph: LoamGraph): Seq[ToolBuilder] = graph.tools.toSeq
  }

  trait LoamSourceRule[Details] extends LoamRule[(StoreBuilder, LoamGraph.StoreSource), Details] {
    override def targets(graph: LoamGraph): Seq[(StoreBuilder, LoamGraph.StoreSource)] = graph.storeSources.toSeq
  }

  trait LoamConsumerRule[Details] extends LoamRule[(StoreBuilder, ToolBuilder), Details] {
    override def targets(graph: LoamGraph): Seq[(StoreBuilder, ToolBuilder)] =
      graph.storeConsumers.flatMap({ case (store, tools) => tools.map((store, _)) }).toSeq
  }

  val eachStoreHasASource = new LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: StoreBuilder): Seq[LoamStoreIssue[Unit]] =
      issueIf(graph.storeSources.get(store).isEmpty,
        newIssue[StoreBuilder, Unit](graph, this, store, (), Severity.Error, s"No source for store $store"))
  }

  val eachToolSourcedStoreIsOutputOfThatTool = new LoamSourceRule[Unit] {
    override def apply(graph: LoamGraph, sourceEntry: (StoreBuilder, LoamGraph.StoreSource)):
    Seq[LoamSourceIssue[Unit]] = sourceEntry match {
      case (store, fromTool: LoamGraph.StoreSource.FromTool) =>
        val tool = fromTool.tool
        issueIfElseIf(!graph.tools(tool),
          newIssue[(StoreBuilder, LoamGraph.StoreSource), Unit](graph, this, (store, fromTool), (), Severity.Error,
            s"Store $store has as source tool $tool, but this tool is not part of the graph."),
          !graph.toolOutputs.getOrElse(tool, Set.empty).contains(store),
          newIssue[(StoreBuilder, LoamGraph.StoreSource), Unit](graph, this, (store, fromTool), (), Severity.Error,
            s"Store $store has as source tool $tool, but is not an output of that tool."))
      case _ => Seq.empty
    }
  }

  val eachStoresIsInputOfItsConsumers = new LoamConsumerRule[Unit] {
    override def apply(graph: LoamGraph, consumerEntry: (StoreBuilder, ToolBuilder)):
    Seq[LoamConsumerIssue[Unit]] = {
      val (store, tool) = consumerEntry
      issueIf(!graph.toolInputs.getOrElse(tool, Set.empty).contains(store),
        newIssue[(StoreBuilder, ToolBuilder), Unit](graph, this, consumerEntry, (), Severity.Error,
          s"Tool $tool is consumer of store $store, but the store is not among its inputs."))
    }
  }

  val eachToolsInputStoresArePresent = new LoamToolRule[Set[StoreBuilder]] {
    override def apply(graph: LoamGraph, tool: ToolBuilder): Seq[LoamToolIssue[Set[StoreBuilder]]] = {
      val missingInputs = graph.toolInputs.getOrElse(tool, Set.empty) -- graph.stores
      issueIf(missingInputs.nonEmpty,
        newIssue[ToolBuilder, Set[StoreBuilder]](graph, this, tool, missingInputs, Severity.Error,
          s"The following inputs of tool $tool are missing from the graph: ${missingInputs.mkString(", ")}"))
    }
  }

  val eachToolsOutputStoresArePresent = new LoamToolRule[Set[StoreBuilder]] {
    override def apply(graph: LoamGraph, tool: ToolBuilder): Seq[LoamToolIssue[Set[StoreBuilder]]] = {
      val missingOutputs = graph.toolOutputs.getOrElse(tool, Set.empty) -- graph.stores
      issueIf(missingOutputs.nonEmpty,
        newIssue[ToolBuilder, Set[StoreBuilder]](graph, this, tool, missingOutputs, Severity.Error,
          s"The following outputs of tool $tool are missing from the graph: ${missingOutputs.mkString(", ")}"))
    }
  }

  val allRules = Seq(eachStoreHasASource, eachToolSourcedStoreIsOutputOfThatTool, eachStoresIsInputOfItsConsumers,
    eachToolsInputStoresArePresent, eachToolsOutputStoresArePresent)

}

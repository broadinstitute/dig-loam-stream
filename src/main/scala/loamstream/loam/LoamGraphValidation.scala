package loamstream.loam

import loamstream.util.Validator.{GlobalIssue, GlobalRule, Issue, LocalIssue, LocalRule, Rule, Severity}

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object LoamGraphValidation {

  type LoamRule = Rule[LoamGraph]
  type LoamGlobalRule = GlobalRule[LoamGraph]
  type LoamLocalRule[Location] = LocalRule[LoamGraph, Location]
  type LoamIssue = Issue[LoamGraph]
  type LoamGlobalIssue = GlobalIssue[LoamGraph]
  type LoamStoreIssue = LocalIssue[LoamGraph, StoreBuilder]
  type LoamToolIssue = LocalIssue[LoamGraph, ToolBuilder]

  def storeIssue(graph: LoamGraph, store: StoreBuilder, rule: LoamLocalRule[StoreBuilder], severity: Severity,
                 message: String): LoamStoreIssue =
    LocalIssue[LoamGraph, StoreBuilder](graph, store, rule, severity, message)

  def issueIf[I <: LoamIssue](cond: Boolean, issue: I): Seq[I] = if (cond) Seq(issue) else Seq.empty

  trait LoamRuleByStore extends LoamLocalRule[StoreBuilder] {
    override def locations(graph: LoamGraph): Seq[StoreBuilder] = graph.stores.toSeq
  }

  trait LoamRuleByTool extends LoamLocalRule[ToolBuilder] {
    override def locations(graph: LoamGraph): Seq[ToolBuilder] = graph.tools.toSeq
  }

  val eachStoreHasASource = new LoamRuleByStore {
    override def apply(graph: LoamGraph, store: StoreBuilder): Seq[LocalIssue[LoamGraph, StoreBuilder]] =
      issueIf(graph.storeSources.get(store).isEmpty, storeIssue(graph, store, this, Severity.Error,
        s"No source for $store"))
  }

}

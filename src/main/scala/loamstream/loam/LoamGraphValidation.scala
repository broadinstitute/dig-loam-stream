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

  def storeIssue[Details](graph: LoamGraph, rule: LoamStoreRule[Details], store: StoreBuilder, details: Details,
                          severity: Severity, message: String): LoamStoreIssue[Details] =
    Issue[LoamGraph, StoreBuilder, Details](graph, rule, store, details, severity, message)

  def issueIf[I <: LoamIssue[_, _]](cond: Boolean, issue: I): Seq[I] = if (cond) Seq(issue) else Seq.empty

  def issueIfElseIf[I <: LoamIssue[_, _]](cond1: Boolean, issue1: I, cond2: Boolean, issue2: I): Seq[I] =
    if (cond1) Seq(issue1) else if (cond2) Seq(issue2) else Seq.empty

  trait LoamStoreRule[Details] extends LoamRule[StoreBuilder, Details] {
    override def targets(graph: LoamGraph): Seq[StoreBuilder] = graph.stores.toSeq
  }

  trait LoamToolRule[Details] extends LoamRule[ToolBuilder, Details] {
    override def targets(graph: LoamGraph): Seq[ToolBuilder] = graph.tools.toSeq
  }

  val eachStoreHasASource = new LoamStoreRule[Unit] {
    override def apply(graph: LoamGraph, store: StoreBuilder): Seq[LoamStoreIssue[Unit]] =
      issueIf(graph.storeSources.get(store).isEmpty,
        storeIssue[Unit](graph, this, store, (), Severity.Error, s"No source for $store"))
  }

  val allRules = Seq(eachStoreHasASource)

}

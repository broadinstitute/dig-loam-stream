package loamstream.util

import loamstream.util.Validation.{Composite, Issue, IssueBase}

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
trait Validation[Item] {

  def apply(item: Item): Seq[IssueBase[Item]]

  def ++(oValidation: Validation[Item]): Composite[Item] = oValidation match {
    case Composite(oRules) => Composite(this +: oRules)
    case _ => Composite(Seq(this, oValidation))
  }
}

object Validation {

  trait BulkValidation[Item, Target, Details] extends Validation[Item] {
    def targets(item: Item): Seq[Target]

    override def apply(item: Item): Seq[BulkIssue[Item, Target, Details]] = targets(item).flatMap(apply(item, _))

    def apply(item: Item, target: Target): Seq[BulkIssue[Item, Target, Details]]
  }

  sealed trait Severity

  object Severity {

    case object Warning extends Severity

    case object Error extends Severity

  }

  trait IssueBase[Item] {
    def item: Item

    def rule: Validation[Item]

    def details: Any

    def severity: Severity

    def message: String
  }

  trait Issue[Item, Details] extends IssueBase[Item] {
    def details: Details
  }

  final case class SimpleIssue[Item, Details](item: Item, rule: Validation[Item], details: Details,
                                              severity: Severity, message: String)
    extends Issue[Item, Details]

  trait BulkIssueBase[Item] extends IssueBase[Item] {
    def target: Any
  }

  final case class BulkIssue[Item, Target, Details](item: Item, rule: BulkValidation[Item, Target, Details],
                                                    target: Target, details: Details, severity: Severity,
                                                    message: String)
    extends BulkIssueBase[Item] with Issue[Item, Details]

  final case class Composite[Item](rules: Seq[Validation[Item]]) extends Validation[Item] {

    def apply(item: Item): Seq[IssueBase[Item]] = rules.flatMap(rule => rule(item))

    override def ++(oValidation: Validation[Item]): Composite[Item] = oValidation match {
      case Composite(oRules) => Composite(rules ++ oRules)
      case _ => Composite(rules :+ oValidation)
    }
  }

}

package loamstream.util

import loamstream.util.Validator.{IssueBase, RuleBase}

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object Validator {

  trait RuleBase[Item] {
    def apply(item: Item): Seq[IssueBase[Item]]
  }

  trait Rule[Item, Target, Details] extends RuleBase[Item] {
    def targets(item: Item): Seq[Target]

    override def apply(item: Item): Seq[Issue[Item, Target, Details]] = targets(item).flatMap(apply(item, _))

    def apply(item: Item, target: Target): Seq[Issue[Item, Target, Details]]
  }

  object Rule {

    trait SingleTarget[Item, Details] extends Rule[Item, Item, Details] {
      override def targets(item: Item): Seq[Item] = Seq(item)
    }

    trait NoDetails[Item, Targets] extends Rule[Item, Targets, Unit]

  }

  trait Severity

  object Severity {

    case object Warning extends Severity

    case object Error extends Severity

  }

  trait IssueBase[Item] {
    def item: Item

    def rule: RuleBase[Item]

    def target: Any

    def details: Any

    def severity: Severity

    def message: String
  }

  final case class Issue[Item, Target, Details](item: Item, rule: Rule[Item, Target, Details], target: Target,
                                                details: Details, severity: Severity, message: String)
    extends IssueBase[Item]

}

case class Validator[Item](rules: Seq[RuleBase[Item]]) {

  def validate(item: Item): Seq[IssueBase[Item]] = rules.flatMap(rule => rule(item))

}

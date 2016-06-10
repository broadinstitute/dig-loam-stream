package loamstream.util

import loamstream.util.Validator.{Issue, Rule}

/**
  * LoamStream
  * Created by oliverr on 6/10/2016.
  */
object Validator {

  trait Rule[Item] {
    def apply(item: Item): Seq[Issue[Item]]
  }

  trait GlobalRule[Item] extends Rule[Item] {
    override def apply(item: Item): Seq[GlobalIssue[Item]]
  }

  trait LocalRule[Item, Location] extends Rule[Item] {
    def locations(item: Item): Seq[Location]

    override def apply(item: Item): Seq[LocalIssue[Item, Location]] = locations(item).flatMap(apply(item, _))

    def apply(item: Item, location: Location): Seq[LocalIssue[Item, Location]]
  }

  trait Severity

  case object Warning extends Severity

  case object Error extends Severity

  trait Issue[Item] {
    def item: Item

    def rule: Rule[Item]

    def severity: Severity
  }

  case class GlobalIssue[Item](item: Item, rule: GlobalRule[Item], severity: Severity) extends Issue[Item]

  case class LocalIssue[Item, Location](item: Item, location: Location, rule: LocalRule[Item, Location],
                                        severity: Severity)
    extends Issue

}

case class Validator[Item](rules: Seq[Rule[Item]]) {

  def validate(item: Item): Seq[Issue[Item]] = rules.flatMap(rule => rule(item))

}

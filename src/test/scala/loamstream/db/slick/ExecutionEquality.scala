package loamstream.db.slick

import loamstream.model.jobs.Execution
import org.scalactic.Equality

/**
 * @author clint
 * Sep 12, 2019
 */
object ExecutionEquality {
  object Implicits extends Implicits
  
  trait Implicits {
    implicit object ExecutionEqualityWithoutSettings extends Equality[Execution] {
      override def areEqual(lhs: Execution, b: Any): Boolean = b match {
        case rhs: Execution => equalityFields(lhs) == equalityFields(rhs)
        case _ => false
      }
      
      private def equalityFields(e: Execution): Seq[_] = Seq(
        e.cmd,
        e.envType,
        e.status,
        e.result,
        e.resources,
        e.outputs,
        e.jobDir,
        e.terminationReason)
    }
  }
}

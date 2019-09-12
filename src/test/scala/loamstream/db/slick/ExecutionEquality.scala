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
    private def equalityFields(e: Execution.Persisted): Seq[_] = Seq(
      e.cmd,
      e.envType,
      e.status,
      e.result,
      e.outputs,
      e.jobDir,
      e.terminationReason)
    
    implicit object ExecutionEqualityWithoutSettingsOrResources extends Equality[Execution] {
      override def areEqual(lhs: Execution, b: Any): Boolean = b match {
        case rhs: Execution.Persisted => equalityFields(lhs) == equalityFields(rhs)
        case _ => false
      }
    }
    
    implicit object ExecutionDotPersistedEqualityWithoutSettingsOrResources extends Equality[Execution.Persisted] {
      override def areEqual(lhs: Execution.Persisted, b: Any): Boolean = b match {
        case rhs: Execution.Persisted => equalityFields(lhs) == equalityFields(rhs)
        case _ => false
      }
    }
  }
}

package loamstream.model.jobs.aws

import loamstream.aws.AwsClient
import loamstream.model.execute.Settings
import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.JobNode
import loamstream.model.jobs.LJob

/**
 * @author clint
 * Oct 21, 2019
 */
final case class AwsJob(
    body: AwsClient => Any,
    initialSettings: Settings,
    override val dependencies: Set[JobNode] = Set.empty,
    inputs: Set[DataHandle] = Set.empty,
    outputs: Set[DataHandle] = Set.empty,
    private val nameOpt: Option[String] = None) extends LJob {
  
  override def equals(other: Any): Boolean = other match {
    case that: AwsJob => this.id == that.id
    case _ => false
  }
  
  override def hashCode: Int = id.hashCode
  
  override def name: String = nameOpt.getOrElse(id.toString)
}

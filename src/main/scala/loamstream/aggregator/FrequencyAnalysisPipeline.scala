package loamstream.aggregator

import loamstream.loam.LoamSyntax
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType
import org.broadinstitute.dig.aws.emr.ApplicationConfig
import org.broadinstitute.dig.aws.emr.ClassificationProperties
import org.broadinstitute.dig.aws.JobStep
import loamstream.aws.AwsJobDesc
import loamstream.loam.LoamScriptContext

object FrequencyAnalysisPipeline {
  import LoamSyntax._
  implicit val context: LoamScriptContext = ???
  val params: ProcessorParams = ???
  
  val cluster = Cluster(
      name = params.name.toString,
      instances = 4,
      masterInstanceType = InstanceType.m5_2xlarge,
      slaveInstanceType = InstanceType.m5_2xlarge,
      configurations = Seq(
        ApplicationConfig.sparkEnv.withConfig(ClassificationProperties.sparkUsePython3)
      )
    )

  //def awsClusterJobs(awsJobs: AwsJobDesc*)(implicit context: LoamScriptContext): AwsTool = requireAwsEnv {
  
  val script: URI = params.resources("frequencyAnalysis.py")
  //val script: URI = awsUriOf("resources/pipeline/frequencyanalysis/frequencyAnalysis.py")

  // create the jobs to process each phenotype in parallel
  val jobs = params.outputs.map { phenotype =>
    Seq(JobStep.PySpark(script, phenotype))
  }
  
  awsWith(cluster) {
    awsClusterJobs(jobs: _*)
  }
}

package loamstream.googlecloud

import ClusterConfig.Defaults

/**
 * @author clint
 * Jul 11, 2019
 */
final case class ClusterConfig(
    zone: String = Defaults.zone,
    masterMachineType: String = Defaults.masterMachineType,
    masterBootDiskSize: Int = Defaults.masterBootDiskSize,
    numWorkers: Int = Defaults.numWorkers,
    workerMachineType: String = Defaults.workerMachineType,
    workerBootDiskSize: Int = Defaults.workerBootDiskSize,
    numPreemptibleWorkers: Int = Defaults.numPreemptibleWorkers,
    preemptibleWorkerBootDiskSize: Int = Defaults.preemptibleWorkerBootDiskSize,
    numWorkerLocalSsds: Int = Defaults.numWorkerLocalSsds,
    properties: String = Defaults.properties,
    maxClusterIdleTime: String = Defaults.maxClusterIdleTime)

object ClusterConfig {
  val default: ClusterConfig = ClusterConfig()
  
  object Defaults { // for creating a minimal cluster
    val zone: String = "us-central1-b"
    val masterMachineType: String = "n1-standard-1"
    val masterBootDiskSize: Int = 20 // in GB
    val numWorkers: Int = 2
    val workerMachineType: String = "n1-standard-1"
    val workerBootDiskSize: Int = 20 // in GB
    val numPreemptibleWorkers: Int = 0
    val preemptibleWorkerBootDiskSize: Int = 20 // in GB
    val numWorkerLocalSsds: Int = 0
    val properties: String = {
      "spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M," +
      "spark:spark.driver.memory=45g,spark:spark.driver.maxResultSize=30g,spark:spark.task.maxFailures=20," +
      "spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1"
    }
    val maxClusterIdleTime: String = "10m"
  }
}

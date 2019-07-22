package loamstream.googlecloud

import java.nio.file.Path

import scala.concurrent.duration._
import scala.util.Try

import com.typesafe.config.Config
import loamstream.conf.ValueReaders

import GoogleCloudConfig.Defaults
import loamstream.util.Loggable
import scala.util.Success
import scala.util.Failure
import loamstream.util.Tries


/**
  * @author clint
  * Nov 28, 2016
  */
final case class GoogleCloudConfig(
    gcloudBinary: Path,
    gsutilBinary: Path,
    projectId: String,
    clusterId: String,
    credentialsFile: Path,
    zone: String = Defaults.zone,
    masterMachineType: String = Defaults.masterMachineType,
    masterBootDiskSize: Int = Defaults.masterBootDiskSize,
    numWorkers: Int = Defaults.numWorkers,
    workerMachineType: String = Defaults.workerMachineType,
    workerBootDiskSize: Int = Defaults.workerBootDiskSize,
    numPreemptibleWorkers: Int = Defaults.numPreemptibleWorkers,
    preemptibleWorkerBootDiskSize: Int = Defaults.preemptibleWorkerBootDiskSize,
    imageVersion: String = Defaults.imageVersion,
    scopes: String = Defaults.scopes,
    properties: String = Defaults.properties,
    maxClusterIdleTime: String = Defaults.maxClusterIdleTime)

object GoogleCloudConfig extends Loggable {
  object Defaults { // for creating a minimal cluster
    val zone: String = "us-central1-b"
    val masterMachineType: String = "n1-standard-1"
    val masterBootDiskSize: Int = 20 // in GB
    val numWorkers: Int = 2
    val workerMachineType: String = "n1-standard-1"
    val workerBootDiskSize: Int = 20 // in GB
    val numPreemptibleWorkers: Int = 0
    val preemptibleWorkerBootDiskSize: Int = 20 // in GB
    val imageVersion: String = "1.1.49" // 2.x not supported by Hail, 1.1 needed for new Python API
    val scopes: String = "https://www.googleapis.com/auth/cloud-platform"
    val properties: String = {
      "spark:spark.driver.extraJavaOptions=-Xss4M,spark:spark.executor.extraJavaOptions=-Xss4M," +
      "spark:spark.driver.memory=45g,spark:spark.driver.maxResultSize=30g,spark:spark.task.maxFailures=20," +
      "spark:spark.kryoserializer.buffer.max=1g,hdfs:dfs.replication=1"
    }
    val maxClusterIdleTime: String = "10m"
  }
  
  private final case class Parsed(
      gcloudBinary: Path,
      gsutilBinary: Path,
      projectId: String,
      clusterId: String,
      credentialsFile: Path,
      zone: String = Defaults.zone,
      masterMachineType: String = Defaults.masterMachineType,
      masterBootDiskSize: Int = Defaults.masterBootDiskSize,
      numWorkers: Int = Defaults.numWorkers,
      workerMachineType: String = Defaults.workerMachineType,
      workerBootDiskSize: Int = Defaults.workerBootDiskSize,
      numPreemptibleWorkers: Int = Defaults.numPreemptibleWorkers,
      preemptibleWorkerBootDiskSize: Int = Defaults.preemptibleWorkerBootDiskSize,
      imageVersion: String = Defaults.imageVersion,
      scopes: String = Defaults.scopes,
      properties: String = Defaults.properties,
      maxClusterIdleTime: String = Defaults.maxClusterIdleTime) {
    
    def toGoogleCloudConfig: Try[GoogleCloudConfig] = {
      for {
        _ <- checkMaxClusterIdleTime(maxClusterIdleTime)
      } yield {
        GoogleCloudConfig(
          gcloudBinary = gcloudBinary,
          gsutilBinary = gsutilBinary,
          projectId = projectId,
          clusterId = clusterId,
          credentialsFile = credentialsFile,
          zone = zone,
          masterMachineType = masterMachineType,
          masterBootDiskSize = masterBootDiskSize,
          numWorkers = numWorkers,
          workerMachineType = workerMachineType,
          workerBootDiskSize = workerBootDiskSize,
          numPreemptibleWorkers = numPreemptibleWorkers,
          preemptibleWorkerBootDiskSize = preemptibleWorkerBootDiskSize,
          imageVersion = imageVersion,
          scopes = scopes,
          properties = properties,
          maxClusterIdleTime = maxClusterIdleTime)
      }
    }
  }

  /*
   * See https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/scheduled-deletion
   * for information on this format, plus minimum and maximum values
   */
  private[googlecloud] def checkMaxClusterIdleTime(googleFormatDurationString: String): Try[String] = {
    def toDurationFn(unitPart: Char): Try[Long => Duration] = unitPart match {
      case 's' => Success(_.seconds)
      case 'm' => Success(_.minutes)
      case 'h' => Success(_.hours)
      case 'd' => Success(_.days)
      case u => {
        val message = s"Unit '${u}' is not supported by Google. See " +
        "https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/scheduled-deletion " +
        "for allowed values and constraints."
        
        Tries.failure(message)
      }
    }
    
    def toLong(s: String): Try[Long] = {
      Try(s.toLong) match {
        case s @ Success(_) => s
        case Failure(_) => {
          Tries.failure(s"Error parsing google-format duration string: Couldn't convert '${s}' to a Long")
        }
      }
    }
    
    //See https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/scheduled-deletion
    val minAllowedByGoogle = 10.minutes
    val maxAllowedByGoogle = 14.days
    
    def checkConstraints(d: Duration): Try[Duration] = {
      if(d < minAllowedByGoogle || d > maxAllowedByGoogle) {
        val message = "Error parsing google-format duration string: " +
                     s"Duration ${d} must be >= ${minAllowedByGoogle} and <= ${maxAllowedByGoogle}"
        
        Tries.failure(message)
      } else {
        Success(d)
      }
    }
    
    for {
      unitPart <- Try(googleFormatDurationString.last.toLower)
      amountPart <- Try(googleFormatDurationString.dropRight(1))
      amount <- toLong(amountPart)
      toDuration <- toDurationFn(unitPart)
      duration = toDuration(amount)
      checkedResult <- checkConstraints(duration)
    } yield googleFormatDurationString
  }
  
  def fromConfig(config: Config): Try[GoogleCloudConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader

    //NB: Ficus now marshals the contents of loamstream.googlecloud into a GoogleCloudConfig instance.
    //Names of fields in GoogleCloudConfig and keys under loamstream.googlecloud must match.
    Try(config.as[Parsed]("loamstream.googlecloud")).flatMap(_.toGoogleCloudConfig)
  }
}

package loamstream.googlecloud

import java.nio.file.Path

import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.DurationLong
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.config.Config

import GoogleCloudConfig.Defaults
import loamstream.conf.ValueReaders
import loamstream.util.Loggable
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
    defaultClusterConfig: ClusterConfig = Defaults.clusterConfig)

object GoogleCloudConfig extends Loggable {
  object Defaults { 
    val clusterConfig: ClusterConfig = ClusterConfig.default
  }
  
  private final case class Parsed(
      gcloudBinary: Path,
      gsutilBinary: Path,
      projectId: String,
      clusterId: String,
      credentialsFile: Path,
      defaultClusterConfig: ClusterConfig = Defaults.clusterConfig) {
    
    def toGoogleCloudConfig: Try[GoogleCloudConfig] = {
      for {
        _ <- checkMaxClusterIdleTime(defaultClusterConfig.maxClusterIdleTime)
      } yield {
        GoogleCloudConfig(
          gcloudBinary = gcloudBinary,
          gsutilBinary = gsutilBinary,
          projectId = projectId,
          clusterId = clusterId,
          credentialsFile = credentialsFile,
          defaultClusterConfig = defaultClusterConfig)
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

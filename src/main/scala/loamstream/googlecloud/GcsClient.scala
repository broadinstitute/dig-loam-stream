package loamstream.googlecloud

import java.net.URI
import java.nio.file.Path
import java.time.Instant

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage._
import loamstream.util.HashType.Md5
import loamstream.util.{Hash, Loggable}

/**
 * @author kaan
 * Dec 6, 2016
 *
 * Wrapper around Google Cloud Storage JAVA API to expose methods for job recording purposes
 */
final case class GcsClient(uri: URI, credentialFile: Path) extends Loggable {

  // Instantiate a GCS handle using given credentials.
  // If no credentials provided, attempt to find credentials that might be set in the environment
  private[this] lazy val storage: Storage =
    StorageOptions.newBuilder
      //.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentialFile)))
      .setCredentials(ServiceAccountCredentials.fromStream(java.nio.file.Files.newInputStream(credentialFile)))
      .build
      .getService

  // The name for the bucket
  private[this] lazy val bucketName: String = uri.getHost

  // Path for the storage object
  private[this] lazy val objPath: String = uri.getPath

  // Storage object
  private[this] def obj: Blob = storage.get(bucketName, objPath)

  // Storage object option
  private[this] def objOpt: Option[Blob] = Option(storage.get(bucketName, objPath))

  // MD5 hash of the storage object as String
  private[this] def hashStr: String = obj.getMd5

  // Encapsulated MD5 hash of the storage object
  def hash: Option[Hash] = {
    require(isPresent, s"Object '${uri.toString}' doesn't exist.")
    require(hashStr != null, s"No '${Md5.algorithmName}' hash was available for object '${uri.toString}'") //scalastyle:ignore
    Hash.fromStrings(hashStr, Md5.algorithmName).toOption
  }

  // If the storage object exists
  def isPresent: Boolean = objOpt.isDefined

  // Last update time of the object
  def lastModified: Option[Instant] =
    if (isPresent) { Option(Instant.ofEpochMilli(obj.getUpdateTime)) }
    else { None }

}

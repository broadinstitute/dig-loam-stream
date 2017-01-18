package loamstream.googlecloud

import java.net.URI
import java.nio.file.Paths

import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.storage.{Storage, StorageOptions}
import loamstream.util.HashType.Md5
import loamstream.util.Hash
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: Jan 17, 2016
 */
class GcsClientTest extends FunSuite {
  val credential = Paths.get("/Users/kyuksel/google_credential.json")
  val client = GcsClient.get
  val uri = URI.create("gs://loamstream/qc/pca/out/CAMP.base.hail.pca.tsv")

  test("isPresent") {
    client.isPresent(uri)
  }

  test("hash") {
    import loamstream.util.UriEnrichments._

    val bn = uri.getHost
    val path = uri.getPath
    val pathNoSlash = uri.getPathWithoutLeadingSlash

    val storage: Storage =
      StorageOptions.newBuilder
        .setCredentials(ServiceAccountCredentials.fromStream(java.nio.file.Files.newInputStream(credential)))
        .build
        .getService

    val str = storage.toString
    val opt = storage.getOptions

    //val obj = storage.get(bn, path)
    val obj = storage.get("loamstream", "qc/pca/out/CAMP.base.hail.pca.tsv")

    val hashStr = obj.getMd5

    val hash = Hash.fromStrings(Option(hashStr), Md5.algorithmName)

    true
  }

  test("lastModified") {

  }

}

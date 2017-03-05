package loamstream.googlecloud

import java.net.URI
import java.nio.file.Paths
import java.time.Instant

import loamstream.util.HashType

import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 2/24/17
 */
final class GcsClientTest extends FunSuite {
  val hashAlgorithmName = HashType.Md5.algorithmName

  // scalastyle:off magic.number
  test("blobs") {
    val credentialsPath = Paths.get("/Users/kyuksel/google_credential.json")
    val driver = GcsDriver.fromCredentialsFile(credentialsPath).get
    val client = GcsClient(driver)

    val vcfUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vcf.gz")
    assert(client.hash(vcfUri).get.valueAsBase64String === "f7ts9u2zjfw4h3iabpak2q==")
    assert(client.isPresent(vcfUri))
    assert(client.lastModified(vcfUri).get === Instant.ofEpochMilli(1488559084788L))

    /*BlobMetadata("qc/all/out/CAMP.harmonized.ref.vcf.gz", "Ye0AXWFZA9RPtsBzFxN5Mg==", 1488559084788)*/

    val vcfIndexUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vcf.gz.tbi")
    assert(client.hash(vcfIndexUri).get.valueAsBase64String === "zu7ecyn/3t9uxkxesjqpmq==")
    assert(client.isPresent(vcfIndexUri))
    assert(client.lastModified(vcfIndexUri).get === Instant.ofEpochMilli(1488559113817L))

    /*BlobMetadata("qc/all/out/CAMP.harmonized.ref.vcf.gz.tbi", "E5RH0kG3hZPHjaCA484p4g==", 1488559113817)*/

    val vdsUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vds")
    assert(client.hash(vdsUri).get.valueAsBase64String === "zec0isi7m3skj/lzufg/pa==")
    assert(client.isPresent(vdsUri))
    assert(client.lastModified(vdsUri).get === Instant.ofEpochMilli(1488559298507L))

    /*BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/metadata.json.gz", "DZrDagi/SITICLmpx/Sd5Q==", 1488559285549)
BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/partitioner", "uSk333elXjHjetANfVNzVQ==", 1488559286213)
BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/_SUCCESS", "1B2M2Y8AsgTpgAmY7PhCfg==", 1488559296946)
BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/_common_metadata", "Hhx9zWmOk0pIeF9FVPYrrA==", 1488559298507)
BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/_metadata", "G3S4lTwr09vTIHSszonoIQ==", 1488559297934)
BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/part-r-00000-92f95503-6af7-4f5f-9b7e-b6c8ecefb455.gz.parquet", "7YDjw5xbxd9X8EXNqfUjKg==", 1488559296001)
*/

    val bogusUri = URI.create("gs://some/bogus/uri")
    assert(!client.isPresent(bogusUri))
    assert(client.lastModified(bogusUri) === None)
    assert(client.hash(bogusUri) === None)
  }
/*
  test("unit") {
    val client = GcsClient(Paths.get("dummy"))
  }
  // scalastyle:on magic.number
}


object GcsClientTest {
  private final case class MockGcsDriver(val name: String, val hash: String, val updateTime: Long) extends GcsDriver {

  }
*/
}
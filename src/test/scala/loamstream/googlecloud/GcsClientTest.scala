package loamstream.googlecloud

import java.net.URI
import java.time.Instant

import loamstream.googlecloud.GcsClientTest.MockGcsDriver
import loamstream.util.HashType
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 2/24/17
 */
final class GcsClientTest extends FunSuite {
  val hashAlgorithmName = HashType.Md5.algorithmName

  def testWith(uri: URI,
               blobs: Iterable[BlobMetadata],
               hash: Option[String],
               lastModified: Option[Long],
               isPresent: Boolean = true) = {

    val client = GcsClient(MockGcsDriver(blobs))

    assert(client.hash(uri).map(_.valueAsBase64String) === hash)
    assert(client.isPresent(uri) === isPresent)
    assert(client.lastModified(uri) === lastModified.map(Instant.ofEpochMilli))
  }

  // scalastyle:off magic.number
  // scalastyle:off line.size.limit
  test("hash/isPresent/lastModified") {
    val vcfUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vcf.gz")
    val vcfBlobs = Iterable(
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vcf.gz", "Ye0AXWFZA9RPtsBzFxN5Mg==", 1488559084788L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vcf.gz.tbi", "E5RH0kG3hZPHjaCA484p4g==", 1488559113817L))
    val vcfHash = Some("f7ts9u2zjfw4h3iabpak2q==")
    val vcfLastModified = Some(1488559084788L)

    testWith(vcfUri, vcfBlobs, vcfHash, vcfLastModified)

    val vcfIndexUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vcf.gz.tbi")
    val vcfIndexBlobs = Iterable(
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vcf.gz.tbi", "E5RH0kG3hZPHjaCA484p4g==", 1488559113817L))
    val vcfIndexHash = Some("zu7ecyn/3t9uxkxesjqpmq==")
    val vcfIndexLastModified = Some(1488559113817L)

    testWith(vcfIndexUri, vcfIndexBlobs, vcfIndexHash, vcfIndexLastModified)

    val vdsUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vds")
    val vdsBlobs = Iterable(
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/", "1B2M2Y8AsgTpgAmY7PhCfg==", 1488559284649L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/metadata.json.gz", "DZrDagi/SITICLmpx/Sd5Q==", 1488559285549L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/partitioner", "uSk333elXjHjetANfVNzVQ==", 1488559286213L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/", "1B2M2Y8AsgTpgAmY7PhCfg==", 1488559287391L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/_SUCCESS", "1B2M2Y8AsgTpgAmY7PhCfg==", 1488559296946L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/_common_metadata", "Hhx9zWmOk0pIeF9FVPYrrA==", 1488559298507L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/_metadata", "G3S4lTwr09vTIHSszonoIQ==", 1488559297934L),
      BlobMetadata("qc/all/out/CAMP.harmonized.ref.vds/rdd.parquet/part-r-00000-92f95503-6af7-4f5f-9b7e-b6c8ecefb455.gz.parquet", "7YDjw5xbxd9X8EXNqfUjKg==", 1488559296001L))
    val vdsHash = Some("zec0isi7m3skj/lzufg/pa==")
    val vdsLastModified = Some(1488559298507L)

    testWith(vdsUri, vdsBlobs, vdsHash, vdsLastModified)

    val bogusUri = URI.create("gs://some/bogus/uri")
    val bogusBlobs = Iterable.empty[BlobMetadata]
    val bogusHash = None
    val bogusLastModified = None

    testWith(bogusUri, bogusBlobs, bogusHash, bogusLastModified, isPresent = false)
  }
  // scalastyle:on magic.number
  // scalastyle:off line.size.limit
}


object GcsClientTest {
  private final case class MockGcsDriver(blobs: Iterable[BlobMetadata]) extends CloudStorageDriver {
    override def blobsAt(uRI: URI) = blobs
  }
}

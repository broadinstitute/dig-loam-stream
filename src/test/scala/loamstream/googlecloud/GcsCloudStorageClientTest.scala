package loamstream.googlecloud

import java.net.URI
import java.time.Instant

import loamstream.googlecloud.GcsCloudStorageClientTest.MockGcsDriver
import loamstream.util.HashType
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 2/24/17
 */
final class GcsCloudStorageClientTest extends FunSuite {
  val hashAlgorithmName = HashType.Md5.algorithmName

  private def testWith(uri: URI,
               blobs: Iterable[BlobMetadata],
               expectedHash: Option[String],
               expectedLastModified: Option[Long],
               expectedIsPresent: Boolean = true) = {

    val client = GcsCloudStorageClient(MockGcsDriver(blobs))

    assert(client.hash(uri).map(_.valueAsBase64String) === expectedHash)
    assert(client.isPresent(uri) === expectedIsPresent)
    assert(client.lastModified(uri) === expectedLastModified.map(Instant.ofEpochMilli))
  }

  test("hash/isPresent/lastModified") {
    {
      val vcfAccessTime = 1488559084788L
      val tbiAccessTime = vcfAccessTime + 1
      
      val vcfHash = "Ye0AXWFZA9RPtsBzFxN5Mg=="
      val tbiHash = "E5RH0kG3hZPHjaCA484p4g=="
      
      val vcfUri = URI.create("gs://foo-bucket/foo/bar/baz/b.l.e.r.g")
      val vcfBlobs = Iterable(
        BlobMetadata("foo/bar/baz/b.l.e.r.g", vcfHash, vcfAccessTime),
        BlobMetadata("foo/bar/baz/b.l.e.r.g.tbi", tbiHash, tbiAccessTime))

      testWith(vcfUri, vcfBlobs, Some(vcfHash), Some(vcfAccessTime))
    }

    {
      val tbiModified = 1488559113817L
      val tbiHash = "E5RH0kG3hZPHjaCA484p4g=="
      
      val tbiUri = URI.create("gs://foo-bucket/foo/bar/baz/b.l.e.r.g.tbi")
      val tbiBlobs = Iterable(
        BlobMetadata("foo/bar/baz/b.l.e.r.g.tbi", tbiHash, tbiModified))
  
      testWith(tbiUri, tbiBlobs, Some(tbiHash), Some(tbiModified))
    }

    {
      val accessTime = 1488559298507L
      
      val vdsUri = URI.create("gs://foo-bucket/foo/bar/baz/b.l.e.r.g")
      val vdsBlobs = Iterable(
        BlobMetadata("foo/bar/baz/b.l.e.r.g/", "1B2M2Y8AsgTpgAmY7PhCfg==", accessTime - 1),
        BlobMetadata("foo/bar/baz/b.l.e.r.g/m.json.gz", "DZrDagi/SITICLmpx/Sd5Q==", accessTime - 2),
        BlobMetadata("foo/bar/baz/b.l.e.r.g/part", "uSk333elXjHjetANfVNzVQ==", accessTime - 3),
        BlobMetadata("foo/bar/baz/b.l.e.r.g/rdd.parquet/", "1B2M2Y8AsgTpgAmY7PhCfg==", accessTime - 4),
        BlobMetadata("foo/bar/baz/b.l.e.r.g/rdd.parquet/_SUCCESS", "1B2M2Y8AsgTpgAmY7PhCfg==", accessTime - 5),
        BlobMetadata("foo/bar/baz/b.l.e.r.g/rdd.parquet/_common_metadata", "Hhx9zWmOk0pIeF9FVPYrrA==", accessTime),
        BlobMetadata("foo/bar/baz/b.l.e.r.g/rdd.parquet/_metadata", "G3S4lTwr09vTIHSszonoIQ==", accessTime - 6),
        BlobMetadata("foo/bar/baz/b.l.e.r.g/rdd.parquet/part-r-foo.gz.pq", "7YDjw5xbxd9X8EXNqfUjKg==", accessTime - 7))
      val vdsHash = Some("76MsWjVkglAPwUwFJLGBlw==")
      val vdsLastModified = Some(accessTime)
  
      testWith(vdsUri, vdsBlobs, vdsHash, vdsLastModified)
    }
  
    {
      testWith(URI.create("gs://some/bogus/uri"), Iterable.empty, None, None, expectedIsPresent = false)
    }
  }
}


object GcsCloudStorageClientTest {
  private final case class MockGcsDriver(blobs: Iterable[BlobMetadata]) extends CloudStorageDriver {
    override def blobsAt(uRI: URI) = blobs
  }
}

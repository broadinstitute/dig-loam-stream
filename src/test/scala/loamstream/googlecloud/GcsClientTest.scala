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
    val client = GcsClient.fromCredentialsFile(credentialsPath).get

    val vcfUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vcf.gz")
    assert(client.hash(vcfUri).get.valueAsBase64String === "f7ts9u2zjfw4h3iabpak2q==")
    assert(client.isPresent(vcfUri))
    assert(client.lastModified(vcfUri).get === Instant.ofEpochMilli(1488559084788L))

    val vcfIndexUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vcf.gz.tbi")
    assert(client.hash(vcfIndexUri).get.valueAsBase64String === "zu7ecyn/3t9uxkxesjqpmq==")
    assert(client.isPresent(vcfIndexUri))
    assert(client.lastModified(vcfIndexUri).get === Instant.ofEpochMilli(1488559113817L))

    val vdsUri = URI.create("gs://loamstream/qc/all/out/CAMP.harmonized.ref.vds")
    assert(client.hash(vdsUri).get.valueAsBase64String === "zec0isi7m3skj/lzufg/pa==")
    assert(client.isPresent(vdsUri))
    assert(client.lastModified(vdsUri).get === Instant.ofEpochMilli(1488559298507L))

    val bogusUri = URI.create("gs://some/bogus/uri")
    assert(!client.isPresent(bogusUri))
    assert(client.lastModified(bogusUri) === None)
    assert(client.hash(bogusUri) === None)
  }
  // scalastyle:on magic.number
}
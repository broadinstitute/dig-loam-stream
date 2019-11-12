package loamstream.aws

import org.scalatest.FunSuite
import java.net.URI

/**
 * @author clint
 * Oct 29, 2019
 */
final class S3UriTest extends FunSuite {
  test("unapply") {
    import S3Uri.unapply
    
    def doTest(u: URI, expected: Option[URI]): Unit = {
      assert(unapply(u) === expected)
    }
    
    val s3 = URI.create("s3://foo/bar/baz") 
    
    doTest(s3, Option(s3))
    doTest(URI.create("gs://foo/bar/baz"), None)
    doTest(URI.create("http://example.com/foo/bar/baz"), None)
  }
}

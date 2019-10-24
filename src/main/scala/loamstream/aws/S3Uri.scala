package loamstream.aws

import java.net.URI

/**
 * @author clint
 * Oct 21, 2019
 */
object S3Uri {
  def unapply(uri: URI): Option[URI] = if(uri.getScheme == "s3") Some(uri) else None
}

package loamstream.util

import org.scalatest.FunSuite
import org.broadinstitute.dig.aws.S3

/**
 * @author clint
 * Jul 27, 2018
 */
trait AwsFunSuite extends FunSuite with Loggable {
  protected val bucketName: String = "dig-integration-tests"

  protected def newS3Client: S3Client = new S3Client.Default(bucketName)

  protected val s3Client: S3Client = newS3Client

  def testWithPseudoDir(
      name: String, 
      nukeTestDirOnSuccess: Boolean = true)(body: String => Any): Unit = {
    
    test(name) {
      val mungedName = name.filter(_ != '/')

      val pseudoDirKey = s"integrationTests/${mungedName}"

      def nukeTestDir(): Unit = s3Client.deleteDir(s"${pseudoDirKey}/")

      nukeTestDir()

      body(pseudoDirKey)

      //Test dir will be deleted after successful runs, but will live until the next run
      //if there's a failure.
      if(nukeTestDirOnSuccess) {
        nukeTestDir()
      }
    }
  }

  /*private def uploadResource(resource: String, dirKey: String = "resources"): URI = {
    val key = s"""${dirKey}/${resource.stripPrefix("/")}"""

    //Produce the contents of the classpath resource at `resource` as a string,
    //and will close the InputStream backed by the resource when reading the resource's data is
    //done, either successfully or due to an error.
    val contents: String = {
      using(getClass.getClassLoader.getResourceAsStream(resource)) { stream =>
        using(Source.fromInputStream(stream)) {
          _.mkString.replace("\r\n", "\n")
        }
      }
    }

    logger.debug(s"Uploading $resource to S3...")

    aws.put(key, contents).unsafeRunSync()

    aws.uriOf(key)
  }*/
}

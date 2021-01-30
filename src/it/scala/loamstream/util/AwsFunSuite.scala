package loamstream.util

import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aws.config.AWSConfig
import org.scalatest.FunSuite
import org.broadinstitute.dig.aws.config.S3Config
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId

/**
 * @author clint
 * Jul 27, 2018
 */
trait AwsFunSuite extends FunSuite with Loggable {
  protected val bucketName: String = "dig-integration-tests"

  protected val awsConfig: AWSConfig = {
    //dummy values, except fot the bucket name
    AWSConfig(
        S3Config(bucketName), 
        EmrConfig("some-ssh-key-name", SubnetId("subnet-foo"))) 
  }
  
  protected val aws: AWS = new AWS(awsConfig)

  def testWithPseudoDir(
      name: String, 
      nukeTestDirOnSuccess: Boolean = true)(body: String => Any): Unit = {
    
    test(name) {
      val mungedName = name.filter(_ != '/')

      val pseudoDirKey = s"integrationTests/${mungedName}"

      def nukeTestDir(): Unit = aws.rmdir(s"${pseudoDirKey}/").unsafeRunSync()

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

package loamstream.aws

import loamstream.model.execute.RxExecuter
import loamstream.model.execute.CompositeChunkRunner
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.conf.ExecutionConfig
import org.scalatest.FunSuite
import loamstream.conf.LoamConfig
import loamstream.conf.ConfigParser
import com.typesafe.config.ConfigFactory
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamProjectContext
import loamstream.TestHelpers
import loamstream.loam.LoamSyntax
import loamstream.loam.aws.AwsApi
import loamstream.compiler.LoamEngine
import loamstream.loam.LoamGraph
import java.nio.file.Files
import org.broadinstitute.dig.aws.emr.Cluster
import org.broadinstitute.dig.aws.emr.InstanceType

/**
 * @author clint
 * Oct 21, 2019
 */
final class AwsChunkRunnerTest extends FunSuite {

  test("AWS ops: copy to/from AWS") {
    val executionConfig = ExecutionConfig.default
  
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val localChunkRunner = AsyncLocalChunkRunner(executionConfig)
  
    val configString = """loamstream {
      aws {
        emr {
          subnetId = "subnet-ab89bbf3"
          sshKeyName = "GenomeStore REST"
          securityGroupIds = [ "sg-2b58c961" ]
        }
        s3 { 
          bucket = "dig-integration-tests" 
        }
      }
    }"""
    
    val loamConfig = LoamConfig.fromConfig(ConfigFactory.parseString(configString)).get
    
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty(loamConfig))
    
    val awsBucket = loamConfig.awsConfig.get.s3.bucket
    
    val awsChunkRunner = new AwsChunkRunner(scriptContext.AwsSupport.awsApi)
    
    val executer = RxExecuter.default.copy(runner = new CompositeChunkRunner(Seq(localChunkRunner, awsChunkRunner)))
    
    def withS3Dir[A](testName: String)(body: String => A): A = {
      val mungedName = testName.filter(_ != '/')
    
      val pseudoDirKey = s"integrationTests/${mungedName}"
    
      val aws = scriptContext.AwsSupport.awsApi.asInstanceOf[AwsApi.Default].aws
      
      def nukeTestDir() = {
        println(s"%%%%%%%%% nuking test dir '${pseudoDirKey}'")
        
        aws.rmdir(s"${pseudoDirKey}/")
      }
    
      nukeTestDir()
    
      try {
        body(pseudoDirKey)
      } finally {
        //Test dir will be deleted after successful runs, but will live until the next run
        //if there's a failure.
        nukeTestDir()
      }
    }
    
    val name = "AWS_ops_copy_to_from_AWS"
    
    val inputFile = TestHelpers.path("src/test/resources/a.txt")
    
    TestHelpers.withWorkDir(name) { workDir =>
      
      val outputFile = TestHelpers.path("/home/clint/a.txt")//workDir.resolve("a.txt")
      
      outputFile.toFile.delete()
      
      println(s"%%%%%%%%% output will be written to '${outputFile}'")
      
      withS3Dir(name) { testS3Dir =>
        println(s"%%%%%%%%% output will be written to 's3://${awsBucket}/${testS3Dir}/a.txt'")
        
        import LoamSyntax._

        val src = store(inputFile).asInput
        
        val dest = store(uri(s"s3://${awsBucket}/${testS3Dir}/a.txt"))
        
        val finalDest = store(outputFile)
  
        val cluster = Cluster(name = name, instances = 1)
        
        awsWith(cluster) {
          awsCopy(src, dest)
          
          awsCopy(dest, finalDest)
        }
        
        val graph = scriptContext.projectContext.graph
        
        import Files.exists
      
        assert(exists(outputFile) === false)
      
        val executable = LoamEngine.toExecutable(graph, awsApi = Some(scriptContext.AwsSupport.awsApi))
      
        executer.execute(executable)
        
        assert(exists(outputFile) === true)
        
        import loamstream.util.Files.readFrom
        
        assert(readFrom(inputFile) === readFrom(outputFile))
      }
    }
  }
}

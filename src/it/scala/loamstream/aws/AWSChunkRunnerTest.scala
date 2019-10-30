package loamstream.aws

import java.nio.file.Files.exists
import java.nio.file.Path
import org.broadinstitute.dig.aws.AWS
import org.scalatest.FunSuite
import com.typesafe.config.ConfigFactory
import loamstream.compiler.LoamEngine
import loamstream.conf.ExecutionConfig
import loamstream.conf.LoamConfig
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScriptContext
import loamstream.model.execute.AsyncLocalChunkRunner
import loamstream.model.execute.CompositeChunkRunner
import loamstream.model.execute.RxExecuter
import loamstream.util.Files.readFrom
import loamstream.loam.LoamSyntax
import loamstream.IntegrationTestHelpers


/**
 * @author clint
 * Oct 21, 2019
 */
final class AWSChunkRunnerTest extends FunSuite {

  test("AWS ops: copy to/from AWS") {
    val executionConfig = ExecutionConfig.default
  
    import scala.concurrent.ExecutionContext.Implicits.global
    
    val localChunkRunner = AsyncLocalChunkRunner(executionConfig)
  
    val loamConfig = LoamConfig.fromConfig(ConfigFactory.parseString(configString)).get
    
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty(loamConfig))
    
    val awsBucket = loamConfig.awsConfig.get.s3.bucket
    
    val awsClient = AwsClient.Default(new AWS(loamConfig.awsConfig.get))
    
    val awsChunkRunner = new AwsChunkRunner(awsClient)
    
    val executer = RxExecuter.default.copy(runner = new CompositeChunkRunner(Seq(localChunkRunner, awsChunkRunner)))
    
    val name = "AWS_ops_copy_to_from_AWS"
    
    val inputFile = IntegrationTestHelpers.path("src/test/resources/a.txt")
    
    withWorkAndS3Dir(name, awsClient.aws) { (workDir, testS3Dir) =>
      val outputFile = workDir.resolve("a.txt")
      
      outputFile.toFile.delete()
      
      {
        import LoamSyntax._
  
        val localSrc = store(inputFile).asInput
        
        val s3Dest = store(uri(s"s3://${awsBucket}/${testS3Dir}/a.txt"))
        
        val finalLocalDest = store(outputFile)
  
        aws {
          awsCopy(localSrc, s3Dest)
            
          awsCopy(s3Dest, finalLocalDest)
        }
      }
      
      val graph = scriptContext.projectContext.graph
      
      assert(exists(outputFile) === false)
    
      val executable = LoamEngine.toExecutable(graph, awsClient = Some(awsClient))
    
      executer.execute(executable)
      
      assert(exists(outputFile) === true)
      
      import loamstream.util.Files.readFrom
      
      assert(readFrom(inputFile) === readFrom(outputFile))
    }
  }
  
  private val configString = """loamstream {
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
  
  private def withWorkAndS3Dir[A](name: String, aws: AWS)(body: (Path, String) => A): A = {
    IntegrationTestHelpers.withWorkDirUnderTarget() { workDir =>
      withS3Dir(name, aws) { testS3Dir =>
        body(workDir, testS3Dir)
      }
    }
  }
  
  private def withS3Dir[A](testName: String, aws: AWS)(body: String => A): A = {
    val mungedName = testName.filter(_ != '/')
  
    val pseudoDirKey = s"integrationTests/${mungedName}"
  
    def nukeTestDir() = aws.rmdir(s"${pseudoDirKey}/").unsafeRunSync()
  
    nukeTestDir()
  
    val result = body(pseudoDirKey)
    
    //Test dir will be deleted after successful runs, but will live until the next run
    //if there's a failure.
    nukeTestDir()
    
    result
  }
}

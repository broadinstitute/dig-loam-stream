package loamstream.loam

import org.scalatest.FunSuite
import loamstream.compiler.LoamCompiler
import loamstream.util.Loggable
import loamstream.model.execute.RxExecuter
import loamstream.compiler.LoamEngine
import loamstream.model.execute.Environment
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.LJob
import loamstream.model.jobs.NoOpJob
import loamstream.TestHelpers
import loamstream.conf.UgerSettings
import loamstream.conf.GoogleSettings
import loamstream.model.execute.EnvironmentType
import loamstream.model.execute.Environment
import loamstream.compiler.LoamPredef
import loamstream.model.quantities.Memory
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.CpuTime

/**
 * @author clint
 * Nov 22, 2016
 */
final class LoamEnvironmentTest extends FunSuite with Loggable {
  
  private val loamCompiler = LoamCompiler.default
  
  private val loamEngine = LoamEngine(TestHelpers.config, loamCompiler, RxExecuter.default)
  
  private val clusterId = TestHelpers.config.googleConfig.get.clusterId
  
  test("Default EE") {
    val graph = TestHelpers.makeGraph { implicit context => 
      import LoamCmdTool._
      
      cmd"foo"
    }
    
    val executable = LoamEngine.toExecutable(graph)
    
    val job = executable.jobs.head
    
    assert(executable.jobs.size === 1)
    assert(job.asInstanceOf[CommandLineStringJob].commandLineString === "foo")
    assert(job.executionEnvironment === Environment.Local)
  }
  
  test("Set EE") {
    def doTest(env: Environment): Unit = {
      val methodName = env.toString.toLowerCase
      
      val graph = TestHelpers.makeGraph { implicit context => 
        import LoamCmdTool._
      
        envFn(env) {
          cmd"foo"
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
      
      val job = executable.jobs.head
      
      assert(executable.jobs.size === 1)
      assert(job.asInstanceOf[CommandLineStringJob].commandLineString === "foo")  
      assert(job.executionEnvironment === env)
    }
    
    doTest(Environment.Local)
    doTest(Environment.Uger(UgerSettings(Cpus(2), Memory.inGb(3), CpuTime.inHours(4))))
    doTest(Environment.Google(GoogleSettings(clusterId)))
  }
  
  test("Multiple EEs") {
    def doTest(env1: Environment, env2: Environment, env3: Environment ): Unit = {
      val methodName1 = env1.toString.toLowerCase
      val methodName2 = env2.toString.toLowerCase
      val methodName3 = env3.toString.toLowerCase
      
      val graph = TestHelpers.makeGraph { implicit context => 
        import LoamCmdTool._
        
        envFn(env1) {
          cmd"foo"
          cmd"bar"
        }
        
        envFn(env2) {
          cmd"baz"
        }
        
        envFn(env3) {
          cmd"blerg"
          cmd"zerg"
        }
      }
      
      val executable = LoamEngine.toExecutable(graph)
      
      //NB: Drop NoOpJob
      val jobs = executable.jobs.head.inputs
      
      def jobWith(commandLine: String): LJob = {
        jobs.find(_.asInstanceOf[CommandLineStringJob].commandLineString == commandLine).get
      }
      
      val foo = jobWith("foo")
      val bar = jobWith("bar")
      val baz = jobWith("baz")
      val blerg = jobWith("blerg")
      val zerg = jobWith("zerg")
      
      assert(jobs.size === 5)
        
      assert(foo.executionEnvironment === env1)
      assert(bar.executionEnvironment === env1)
      
      assert(baz.executionEnvironment === env2)
      
      assert(blerg.executionEnvironment === env3)
      assert(zerg.executionEnvironment === env3)
    }
    
    import Environment._
    
    val ugerEnv = Uger(UgerSettings.Defaults)
    val googleEnv = Google(GoogleSettings(clusterId))
    
    doTest(Local, ugerEnv, googleEnv)
    doTest(googleEnv, ugerEnv, Local)
    doTest(Local, Local, Local)
    doTest(googleEnv, googleEnv, googleEnv)
    doTest(ugerEnv, ugerEnv, ugerEnv)
    doTest(googleEnv, ugerEnv, ugerEnv)
    doTest(ugerEnv, Local, Local)
    doTest(Local, ugerEnv, Local)
    
  }
  
  private def envFn[A](env: Environment)(block: => A)(implicit context: LoamScriptContext): A = env match {
    case Environment.Local => LoamPredef.local(block)
    case Environment.Google(_) => LoamPredef.google(block)
    case Environment.Uger(settings) => {
      LoamPredef.ugerWith(settings.cpus.value, settings.memoryPerCpu.gb, settings.maxRunTime.hours)(block)
    }
  }
}

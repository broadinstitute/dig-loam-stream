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

/**
 * @author clint
 * Nov 22, 2016
 */
final class LoamEnvironmentTest extends FunSuite with Loggable {
  
  private val loamCompiler = LoamCompiler.default
  
  private val loamEngine = LoamEngine(TestHelpers.config, loamCompiler, RxExecuter.default)
  
  test("Default EE") {
    val code = """
                 cmd"foo"
               """
    
    val executable = loamEngine.compileToExecutable(code).get
    
    val job = executable.jobs.head
    
    assert(executable.jobs.size === 1)
    assert(job.asInstanceOf[CommandLineStringJob].commandLineString === "foo")
    assert(job.executionEnvironment === Environment.Local)
  }
  
  test("Set EE") {
    def doTest(env: Environment): Unit = {
      val methodName = env.toString.toLowerCase
      
      val code = s"""
                    $methodName {
                      cmd"foo"
                    }
                  """
      
      val executable = loamEngine.compileToExecutable(code).get
      
      val job = executable.jobs.head
      
      assert(executable.jobs.size === 1)
      assert(job.asInstanceOf[CommandLineStringJob].commandLineString === "foo")  
      assert(job.executionEnvironment === env)
    }
    
    doTest(Environment.Local)
    doTest(Environment.Uger(UgerSettings.Defaults))
    doTest(Environment.Google)
  }
  
  test("Multiple EEs") {
    def doTest(env1: Environment, env2: Environment, env3: Environment ): Unit = {
      val methodName1 = env1.toString.toLowerCase
      val methodName2 = env2.toString.toLowerCase
      val methodName3 = env3.toString.toLowerCase
      
      val code = s"""
                    $methodName1 {
                      cmd"foo"
                      cmd"bar"
                    }
                    $methodName2 {
                      cmd"baz"
                    }
                    $methodName3 {
                      cmd"blerg"
                      cmd"zerg"
                    }
                  """
      
      val executable = loamEngine.compileToExecutable(code).get
      
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
    
    doTest(Local, Uger(UgerSettings.Defaults), Google)
    doTest(Google, Uger(UgerSettings.Defaults), Local)
    doTest(Local, Local, Local)
    doTest(Google, Google, Google)
    doTest(Uger(UgerSettings.Defaults), Uger(UgerSettings.Defaults), Uger(UgerSettings.Defaults))
    doTest(Google, Uger(UgerSettings.Defaults), Uger(UgerSettings.Defaults))
    doTest(Uger(UgerSettings.Defaults), Local, Local)
    doTest(Local, Uger(UgerSettings.Defaults), Local)
    
  }
}

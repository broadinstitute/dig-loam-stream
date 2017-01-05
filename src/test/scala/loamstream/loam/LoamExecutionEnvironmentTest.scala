package loamstream.loam

import org.scalatest.FunSuite
import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.util.Loggable
import loamstream.model.execute.RxExecuter
import loamstream.compiler.LoamEngine
import loamstream.model.execute.ExecutionEnvironment
import loamstream.model.jobs.commandline.CommandLineStringJob
import loamstream.model.jobs.LJob
import loamstream.model.jobs.NoOpJob

/**
 * @author clint
 * Nov 22, 2016
 */
final class LoamExecutionEnvironmentTest extends FunSuite with Loggable {
  
  private def outMessageSink = LoggableOutMessageSink(this)
  
  private val loamCompiler = new LoamCompiler(LoamCompiler.Settings.default, outMessageSink)
  
  private val loamEngine = LoamEngine(loamCompiler, RxExecuter.default, outMessageSink)
  
  test("Default EE") {
    val code = """
                 cmd"foo"
               """
    
    val executable = loamEngine.compileToExecutable(code).get
    
    val job = executable.jobs.head
    
    assert(executable.jobs.size === 1)
    assert(job.asInstanceOf[CommandLineStringJob].commandLineString === "foo")
    assert(job.executionEnvironment === ExecutionEnvironment.Local)
  }
  
  test("Set EE") {
    def doTest(env: ExecutionEnvironment): Unit = {
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
    
    doTest(ExecutionEnvironment.Local)
    doTest(ExecutionEnvironment.Uger)
    doTest(ExecutionEnvironment.Google)
  }
  
  test("Multiple EEs") {
    def doTest(env1: ExecutionEnvironment, env2: ExecutionEnvironment, env3: ExecutionEnvironment ): Unit = {
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
    
    import ExecutionEnvironment._
    
    doTest(Local, Uger, Google)
    doTest(Google, Uger, Local)
    doTest(Local, Local, Local)
    doTest(Google, Google, Google)
    doTest(Uger, Uger, Uger)
    doTest(Google, Uger, Uger)
    doTest(Uger, Local, Local)
    doTest(Local, Uger, Local)
    
  }
}
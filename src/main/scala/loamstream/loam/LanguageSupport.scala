package loamstream.loam

import java.nio.file.Paths
import loamstream.util.Sequence
import loamstream.util.Files
import java.nio.file.Path

/**
 * @author clint
 * Mar 28, 2017
 */
object LanguageSupport  {
  
  object R extends LanguageSupport {
    
    implicit final class StringContextWithR(val stringContext: StringContext) extends AnyVal {
      /*
       * Supports a R string interpolator for .loam files:
       * 
       * import LanguageSupport.R._
       * 
       * val someStore = store[TXT].at("/foo/bar/baz")
       * 
       * r"
       *     sprintf("Hello world! Here's a store: %s", ${someStore} );
       *  "
       */
      def r(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        
        require(
            config.rConfig.isDefined, 
            s"R support requires a valid 'loamstream.R' section in the config file")
            
        val rConfig = config.rConfig.get

        import LoamCmdTool._
        val rScriptContent = stringContext.cmd(args : _*).commandLine
        val scriptFile = determineScriptFile("r", "r")
        Files.writeTo(scriptFile)(rScriptContent)

        cmd"${rConfig.binary} $scriptFile"
      }
    }
  }
  
  object Python extends LanguageSupport {
    
    implicit final class StringContextWithPython(val stringContext: StringContext) extends AnyVal {
      
      /*
       * Supports a Python string interpolator for .loam files:
       * 
       * import LanguageSupport.Python._
       * 
       * val someStore = store[TXT].at("/foo/bar/baz")
       * 
       * python"
       *          print("Hello world! here's a store: ${someStore}")
       *       "
       */
      def python(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        implicit val sc = stringContext

        require(
            config.pythonConfig.isDefined, 
            s"Python support requires a valid 'loamstream.python' section in the config file")
            
        val pythonConfig = config.pythonConfig.get

        import LoamCmdTool._
        val pythonScriptContent = stringContext.cmd(args : _*).commandLine
        val scriptFile = determineScriptFile("python", "py")
        Files.writeTo(scriptFile)(pythonScriptContent)

        cmd"${pythonConfig.binary} $scriptFile"
      }
    }
  }
}

trait LanguageSupport {
  protected[this] val fileNums: Sequence[Int] = Sequence[Int]()
  
  protected[this] def config(implicit scriptContext: LoamScriptContext) = scriptContext.projectContext.config

  protected[this] def determineScriptFile(prefix: String, extension: String)
                                         (implicit scriptContext: LoamScriptContext): Path = {
    val executionId = scriptContext.executionId
    val filename = s"$prefix-$executionId-${fileNums.next()}.$extension"

    // TODO: Allow specifying folder to produce the script file in via the conf file
    Paths.get(".loamstream", filename).toAbsolutePath
  }
}

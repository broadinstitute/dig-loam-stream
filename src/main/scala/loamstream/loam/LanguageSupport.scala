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
       * R"""
       *     sprintf("Hello world! Here's a store: %s", ${someStore} );
       *  """
       */
      def R(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        
        require(
            config.rConfig.isDefined, 
            s"Bash support requires a valid 'loamstream.R' section in the config file")
            
        val rConfig = config.rConfig.get
        
        val scriptFile = writeToFile(args, "R", "r")
        
        import LoamCmdTool._
        
        cmd"${rConfig.binary} $scriptFile"
      }
    }
  }
  
  object Python extends LanguageSupport {
    
    implicit final class StringContextWithBash(val stringContext: StringContext) extends AnyVal {
      
      /*
       * Supports a Python string interpolator for .loam files:
       * 
       * import LanguageSupport.Python._
       * 
       * val someStore = store[TXT].at("/foo/bar/baz")
       * 
       * python"""
       *       print("Hello world! here's a store: ${someStore}")
       *       """
       */
      def python(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        
        require(
            config.pythonConfig.isDefined, 
            s"Bash support requires a valid 'loamstream.python' section in the config file")
            
        val pythonConfig = config.pythonConfig.get
        
        val scriptFile = writeToFile(args, "python", "py")
        
        import LoamCmdTool._
        
        cmd"${pythonConfig.binary} $scriptFile"
      }
    }
  }
}

trait LanguageSupport {
  protected[this] val fileNums: Sequence[Int] = Sequence[Int]()
  
  protected[this] def config(implicit scriptContext: LoamScriptContext) = scriptContext.projectContext.config
  
  protected[this] def piecesToString(args: Seq[Any])(implicit scriptContext: LoamScriptContext): String = {
    val tokens = args.map(LoamCmdTool.toToken) 
    
    LoamCmdTool.toString(scriptContext.projectContext.fileManager, tokens)
  }
  
  protected[this] def determineScriptFile(
      prefix: String, 
      extension: String)(implicit scriptContext: LoamScriptContext): Path = {
    
    val executionId = scriptContext.executionId
    val filename = s"${prefix}-${fileNums.next()}.${extension}"
        
    Paths.get(".loamstream", "executions", executionId, filename).toAbsolutePath
  }
  
  protected[this] def writeToFile(
      args: Seq[Any], 
      prefix: String, 
      extension: String)(implicit scriptContext: LoamScriptContext): Path = {
    
    val scriptContents = piecesToString(args)
      
    val scriptFile = determineScriptFile("bash", "sh")
      
    Files.writeTo(scriptFile)(scriptContents)
    
    scriptFile
  }
}

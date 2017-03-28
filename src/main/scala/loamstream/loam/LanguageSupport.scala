package loamstream.loam

import java.nio.file.Paths
import loamstream.util.Sequence
import loamstream.util.Files
import java.nio.file.Path

/**
 * @author clint
 * Mar 28, 2017
 */
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

object LanguageSupport  {
  
  object Bash extends LanguageSupport {
    
    implicit final class StringContextWithBash(val stringContext: StringContext) extends AnyVal {
      /*
       * Supports a Bash string interpolator for .loam files:
       * 
       * import LanguageSupport.Python._
       * 
       * val someStore = store[TXT].at("/foo/bar/baz")
       * 
       * bash"""
       *       echo "hello world! here's a store: ${someStore}"
       *     """
       */
      def bash(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        
        require(
            config.bashConfig.isDefined, 
            s"Bash support requires a valid 'loamstream.bash' section in the config file")
            
        val bashConfig = scriptContext.projectContext.config.bashConfig.get
        
        val scriptFile = writeToFile(args, "bash", "sh")
        
        import LoamCmdTool._
        
        cmd"${bashConfig.binary} $scriptFile"
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
       *       print("hello world! here's a store: ${someStore}")
       *       """
       */
      def python(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        
        require(
            config.bashConfig.isDefined, 
            s"Bash support requires a valid 'loamstream.python' section in the config file")
            
        val pythonConfig = scriptContext.projectContext.config.pythonConfig.get
        
        val scriptFile = writeToFile(args, "python", "py")
        
        import LoamCmdTool._
        
        cmd"${pythonConfig.binary} $scriptFile"
      }
    }
  }
}

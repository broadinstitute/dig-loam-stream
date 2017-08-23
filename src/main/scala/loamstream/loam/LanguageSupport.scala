package loamstream.loam

import loamstream.util.{Files, PathEnrichments, Sequence, StringUtils}
import java.nio.file.Path

import loamstream.conf.{LoamConfig, PythonConfig, RConfig}
import loamstream.conf.HasScriptDir

/**
 * @author clint
 *         kyuksel
 *         3/28/2017
 */
object LanguageSupport {
  object R {
    private def rConfig(implicit scriptContext: LoamScriptContext): RConfig = {
      require(
        config.rConfig.isDefined,
        s"R support requires a valid 'loamstream.r' section in the config file")

      config.rConfig.get
    }
    
    implicit final class StringContextWithR(val stringContext: StringContext) extends AnyVal {
      /*
       * Supports a R string interpolator for .loam files:
       * 
       * import LanguageSupport.R._
       * 
       * val someStore = store[TXT].at("/foo/bar/baz")
       * 
       *   r"
       * sprintf("Hello world! Here's a store: %s", ${someStore} );
       *   "
       *
       * NOTE: The body of the code must be left-aligned
       */
      def r(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        val scriptFile = makeScript(stringContext, GeneratedScriptParams.r(rConfig), args: _*)
        
        import LoamCmdTool._

        cmd"${rConfig.binary} $scriptFile"
      }
    }
  }
  
  object Python {
    private def pythonConfig(implicit scriptContext: LoamScriptContext): PythonConfig = {
      require(
        config.pythonConfig.isDefined,
        s"Python support requires a valid 'loamstream.python' section in the config file")

      config.pythonConfig.get
    }
    
    implicit final class StringContextWithPython(val stringContext: StringContext) extends AnyVal {
      /*
       * Supports a Python string interpolator for .loam files:
       * 
       * import LanguageSupport.Python._
       * 
       * val someStore = store[TXT].at("/foo/bar/baz")
       * 
       *  python"
       * print("Hello world! here's a store: ${someStore}")
       *  "
       *
       * NOTE: The body of the code must be left-aligned
       */
      def python(args: Any*)(implicit scriptContext: LoamScriptContext): LoamCmdTool = {
        val scriptFile = makeScript(stringContext, GeneratedScriptParams.python(pythonConfig), args: _*)
        
        import LoamCmdTool._

        cmd"${pythonConfig.binary} $scriptFile"
      }
    }
  }
  
  private[this] val fileNums: Sequence[Int] = Sequence[Int]()
  
  def config(implicit scriptContext: LoamScriptContext): LoamConfig = {
    scriptContext.projectContext.config
  }
  
  def determineScriptFile(prefix: String, extension: String, dir: Path)
                                         (implicit scriptContext: LoamScriptContext): Path = {
    import PathEnrichments._

    val executionId = scriptContext.executionId
    val filename = s"$prefix-$executionId-${fileNums.next()}.$extension"

    Files.createDirsIfNecessary(dir)

    dir/filename
  }
  
  def makeScriptContent(
      stringContext: StringContext, 
      args: Any*)(implicit scriptContext: LoamScriptContext): String = {
    
    LoamCmdTool.create(args: _*)(StringUtils.assimilateLineBreaks)(scriptContext, stringContext).commandLine
  }
  
  final class GeneratedScriptParams(val prefix: String, val extension: String, val outputDir: Path) {
    def this(prefix: String, extension: String, config: HasScriptDir) = this(prefix, extension, config.scriptDir)
  }
  
  object GeneratedScriptParams {
    def apply(prefix: String, extension: String, outputDir: Path): GeneratedScriptParams = {
      new GeneratedScriptParams(prefix, extension, outputDir)
    }
    
    def python(config: PythonConfig): GeneratedScriptParams = new GeneratedScriptParams("python", "py", config)
    
    def r(config: RConfig): GeneratedScriptParams = new GeneratedScriptParams("R", "r", config)
  }
  
  def makeScript(
      stringContext: StringContext, 
      params: GeneratedScriptParams,
      args: Any*)(implicit scriptContext: LoamScriptContext): Path = {
    
    val scriptContent = makeScriptContent(stringContext, args: _*)
        
    val scriptFile = determineScriptFile(params.prefix, params.extension, params.outputDir)
        
    Files.writeTo(scriptFile)(scriptContent)
    
    scriptFile
  }
}

package loamstream.loam

import loamstream.util.{Files, PathEnrichments, Sequence, StringUtils}
import java.nio.file.Path

import loamstream.conf.{LoamConfig, PythonConfig, RConfig}

/**
 * @author clint
 *         kyuksel
 *         3/28/2017
 */
trait LanguageSupport {
  protected[this] val fileNums: Sequence[Int] = Sequence[Int]()

  protected[this] def config(implicit scriptContext: LoamScriptContext): LoamConfig = {
    scriptContext.projectContext.config
  }

  protected[this] def determineScriptFile(prefix: String, extension: String, dir: Path)
                                         (implicit scriptContext: LoamScriptContext): Path = {
    import PathEnrichments._

    val executionId = scriptContext.executionId
    val filename = s"$prefix-$executionId-${fileNums.next()}.$extension"

    Files.createDirsIfNecessary(dir)

    dir/filename
  }
}

object LanguageSupport {
  object R extends LanguageSupport {
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
        import LoamCmdTool._

        val scriptContent = {
          LoamCmdTool.create(args: _*)(StringUtils.assimilateLineBreaks)(scriptContext, stringContext).commandLine
        }
        
        val scriptFile = determineScriptFile("R", "r", rConfig.scriptDir)
        
        Files.writeTo(scriptFile)(scriptContent)

        cmd"${rConfig.binary} $scriptFile"
      }
    }
  }
  
  object Python extends LanguageSupport {
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
        import LoamCmdTool._

        val scriptContent = {
          LoamCmdTool.create(args: _*)(StringUtils.assimilateLineBreaks)(scriptContext, stringContext).commandLine
        }
        
        val scriptFile = determineScriptFile("python", "py", pythonConfig.scriptDir)
        
        Files.writeTo(scriptFile)(scriptContent)

        cmd"${pythonConfig.binary} $scriptFile"
      }
    }
  }
}

package loamstream.loam

import loamstream.util.{Files, PathEnrichments, Sequence}
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

    createIfNecessary(dir)

    dir/filename
  }

  private def createIfNecessary(directory: Path): Unit = {
    val dir = directory.toFile

    if (! dir.exists) dir.mkdirs()

    assert(dir.exists)
  }
}

object LanguageSupport {
  object R extends LanguageSupport {
    private def rConfig(implicit scriptContext: LoamScriptContext): RConfig = {
      require(
        super.config.rConfig.isDefined,
        s"R support requires a valid 'loamstream.r' section in the config file")

      super.config.rConfig.get
    }

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
        import LoamCmdTool._

        val rScriptContent = stringContext.cmd(args : _*).commandLine
        val rScriptFile = determineScriptFile("r", "r", rConfig.scriptDir)
        Files.writeTo(rScriptFile)(rScriptContent)

        cmd"${rConfig.binary} $rScriptFile"
      }
    }
  }
  
  object Python extends LanguageSupport {
    private def pythonConfig(implicit scriptContext: LoamScriptContext): PythonConfig = {
      require(
        super.config.pythonConfig.isDefined,
        s"Python support requires a valid 'loamstream.python' section in the config file")

      super.config.pythonConfig.get
    }

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
        import LoamCmdTool._

        val pythonScriptContent = stringContext.cmd(args : _*).commandLine
        val pythonScriptFile = determineScriptFile("python", "py", pythonConfig.scriptDir)
        Files.writeTo(pythonScriptFile)(pythonScriptContent)

        cmd"${pythonConfig.binary} $pythonScriptFile"
      }
    }
  }
}

package loamstream.loam

import java.io.File
import java.nio.file.{Path, Paths}

import loamstream.compiler.LoamPredef.store
import loamstream.TestHelpers
import loamstream.util.{BashScript, Files}
import org.scalatest.FunSuite

/**
 * @author kyuksel
 *         date: 4/11/17
 */
final class LanguageSupportTest extends FunSuite {
  import TestHelpers.config

  // Since the cmd"..." scripts are run through `sh` the file separator
  // should be what is understood by that executable and not platform
  // specific.
  private val bashFileSep = '/' //BashScript.escapeString(File.separator)

  private implicit val scriptContext = new LoamScriptContext(emptyProjectContext)

  private def emptyProjectContext = LoamProjectContext.empty(config)

  private def doTest(loamLine: LoamCmdTool,
                     expectedBinary: Path,
                     expectedScriptContent: String): Unit = {

    val commandLine = LoamCmdTool.toString(loamLine.tokens)
    val pieces = commandLine.split(" ")
    val binary = pieces.head
    val file = pieces.last
    val scriptContent = Files.readFrom(file)

    assert(binary === expectedBinary.toString)
    assert(scriptContent === expectedScriptContent)
  }

  test("Python - empty snippet") {
    import LanguageSupport.Python._

    val loamLine = python""
    val expectedBinary = Paths.get("/path/to/python/binary")
    val expectedScriptContent = ""

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }

  test("Python - one-liner") {
    import LanguageSupport.Python._

    val someTool = "someToolPath"
    val someVal = 123
    val someStore = store.at("/someStorePath")

    val loamLine = python"""$someTool --foo $someVal --bar $someStore baz"""

    val expectedBinary = Paths.get("/path/to/python/binary")
    val expectedScriptContent = s"someToolPath --foo 123 --bar ${bashFileSep}someStorePath baz"

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }

  test("Python - multi-liner") {
    import LanguageSupport.Python._
    val Alice = "Alice"
    val Bob = "Bob"

    val loamLines =
python"""
def greet(name):
  print 'Hello', name

greet('$Alice')
greet('$Bob')
"""

    val expectedBinary = Paths.get("/path/to/python/binary")
    val expectedScriptContent =
"""
def greet(name):
  print 'Hello', name

greet('Alice')
greet('Bob')
"""

    doTest(loamLines, expectedBinary, expectedScriptContent)
  }

  test("R - empty snippet") {
    import LanguageSupport.R._

    val loamLine = r""""""
    val expectedBinary = Paths.get("/path/to/R/binary")
    val expectedScriptContent = ""

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }

  test("R - one-liner") {
    import LanguageSupport.R._

    val someTool = "someToooolPath"
    val someVal = 456
    val someStore = store.at("/someStooorePath")

    val loamLine = r"$someTool --foo $someVal --bar $someStore baz"

    val expectedBinary = Paths.get("/path/to/R/binary")
    val expectedScriptContent = s"someToooolPath --foo 456 --bar ${bashFileSep}someStooorePath baz"

    doTest(loamLine, expectedBinary, expectedScriptContent)
  }

  test("R - multi-liner") {
    import LanguageSupport.R._

    val loamLines =
r"""
args<-commandArgs(trailingOnly=T)
x<-try(read.table(args[1],header=T,as.is=T,stringsAsFactors=F), silent=TRUE)

if(inherits(x, "try-error")) {
  file.create(args[2])
} else {
  ids<-c(x$$ID1,x$$ID2)
  out<-as.data.frame(sort(table(ids),decreasing=T))
  names(out)[1]<-"ibd_pairs"
  write.table(out,args[2],row.names=T,col.names=F,quote=F,append=F,sep="\t")
}
"""

    val expectedBinary = Paths.get("/path/to/R/binary")
    val expectedScriptContent =
"""
args<-commandArgs(trailingOnly=T)
x<-try(read.table(args[1],header=T,as.is=T,stringsAsFactors=F), silent=TRUE)

if(inherits(x, "try-error")) {
  file.create(args[2])
} else {
  ids<-c(x$ID1,x$ID2)
  out<-as.data.frame(sort(table(ids),decreasing=T))
  names(out)[1]<-"ibd_pairs"
  write.table(out,args[2],row.names=T,col.names=F,quote=F,append=F,sep="\t")
}
"""

    doTest(loamLines, expectedBinary, expectedScriptContent)
  }
}

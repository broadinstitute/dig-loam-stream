package loamstream.web.parser

import java.io.File

import loamstream.web.controllers.socket.CompilerOutMessage.Severity
import loamstream.web.controllers.socket.SocketMessageHandler.OutMessageSink
import loamstream.web.controllers.socket.{CompilerOutMessage, ErrorOutMessage, ReceiptOutMessage, StatusOutMessage}
import loamstream.web.parser.LoamCompiler.CompilerReporter

import scala.reflect.internal.util.{BatchSourceFile, Position}
import scala.tools.nsc.Settings
import scala.tools.nsc.interactive.{Global, Response}
import scala.tools.nsc.io.VirtualDirectory
import scala.tools.nsc.reporters.Reporter

/**
  * LoamStream
  * Created by oliverr on 5/10/2016.
  */
object LoamCompiler {

  class CompilerReporter(outMessageSink: OutMessageSink) extends Reporter {
    override protected def info0(pos: Position, msg: String, severity: Severity, force: Boolean): Unit = {
      outMessageSink.send(CompilerOutMessage(pos, msg, Severity(severity.id), force))
    }
  }

}

class LoamCompiler(outMessageSink: OutMessageSink) {

  val targetDirectoryName = "target"
  val targetDirectoryParentOption = None
  val targetDirectory = new VirtualDirectory(targetDirectoryName, targetDirectoryParentOption)
  val settings = new Settings()
  settings.outputDirs.setSingleOutput(targetDirectory)
  val sbtClasspath = System.getProperty("sbt-classpath")
  settings.classpath.value = s".${File.pathSeparator}$sbtClasspath"
  val reporter = new CompilerReporter(outMessageSink)
  val compiler = new Global(settings, reporter)
  val sourceFileName = "Config.scala"

  def compile(codeString: String): Unit = {
    val sourceFile = new BatchSourceFile(sourceFileName, codeString)
    val response = new Response[Unit]
    compiler.askReload(List(sourceFile), response)
    outMessageSink.send(StatusOutMessage(s"Compiler reloaded and we got response '$response'."))
  }

}

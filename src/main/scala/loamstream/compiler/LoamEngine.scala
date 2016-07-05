package loamstream.compiler

import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Paths, Files => JFiles}

import loamstream.compiler.messages.{ClientMessageHandler, ErrorOutMessage, StatusOutMessage}
import loamstream.model.execute.ChunkedExecuter
import loamstream.util.{Hit, Miss, Shot}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Try


/**
  * LoamStream
  * Created by oliverr on 7/5/2016.
  */
case class LoamEngine(outMessageSink: ClientMessageHandler.OutMessageSink) {

  def report[T](shot: Shot[T], statusMsg: T => String): Unit = {
    val message = shot match {
      case Hit(item) => StatusOutMessage(statusMsg(item))
      case miss: Miss => ErrorOutMessage(miss.toString)
    }
    outMessageSink.send(message)
  }

  val compiler = new LoamCompiler(outMessageSink)(global)
  val executer = ChunkedExecuter.default

  def loadFile(fileName: String): Shot[String] = loadFile(Paths.get(fileName))

  def loadFile(file: Path): Shot[String] = {
    val fileShot = if (JFiles.exists(file)) {
      Hit(file)
    } else if (!file.toString.endsWith(".loam")) {
      val alternateFile = Paths.get(file.toString + ".loam")
      if (JFiles.exists(alternateFile)) {
        Hit(alternateFile)
      } else {
        Miss(s"Could not find '$file' nor '$alternateFile'.")
      }
    } else {
      Miss(s"Could not find '$file'.")
    }
    fileShot.flatMap(file => Shot.fromTry[String](Try {
      new String(JFiles.readAllBytes(file), StandardCharsets.UTF_8)
    }))
  }

  def compile(file: Path): Shot[LoamCompiler.Result] = loadFile(file).map(compile(_))

  def compile(code: String): LoamCompiler.Result = compiler.compile(code)

  def run(file: Path): Unit = loadFile(file).map(run(_))

  def run(code: String): Unit = {
    ???
  }

}

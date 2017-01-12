package loamstream.compiler.messages

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.compiler.{Issue, LoamCompiler, LoamEngine}
import loamstream.loam.LoamScript
import loamstream.util.{Hit, Loggable, Miss}

import scala.concurrent.ExecutionContext
import java.nio.file.Paths

/** The handler responding to messages sent by a client */
object ClientMessageHandler {

  /** A receiver of messages sent to a client */
  object OutMessageSink {

    /** A receiver of messages that does nothing */
    object NoOp extends OutMessageSink {
      override def send(outMessage: ClientOutMessage): Unit = ()
    }

    /** A receiver of messages that logs messages */
    case class LoggableOutMessageSink(loggable: Loggable) extends OutMessageSink {
      /** Accepts messages and logs them */
      override def send(outMessage: ClientOutMessage): Unit = {
        outMessage match {
          case ErrorOutMessage(message) => loggable.error(message)
          case CompilerIssueMessage(issue) => issue.severity match {
            case Issue.Severity.Info => loggable.info(issue.msg)
            case Issue.Severity.Warning => loggable.warn(issue.msg)
            case Issue.Severity.Error => loggable.error(issue.msg)
          }
          case _ => loggable.info(outMessage.message)
        }
      }
    }

  }

  /** A receiver of messages sent to a client */
  trait OutMessageSink {
    /** Accepts messages to be sent to the client */
    def send(outMessage: ClientOutMessage)
  }

}

/** The handler responding to messages sent by a client */
final case class ClientMessageHandler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext) {
  
  val repo = {
    val exampleDir = Paths.get("src/main/loam/examples")
    
    LoamRepository.inMemory ++ LoamRepository.ofFolder(exampleDir)
  }
  
  val engine = LoamEngine.default(outMessageSink)

  def compile(code: String): Unit = {
    outMessageSink.send(ReceiptOutMessage(code))
    engine.compile(code)
  }

  def run(code: String): Unit = {
    outMessageSink.send(ReceiptOutMessage(code))
    engine.run(code)
  }

  def load(name: String): Unit = repo.load(name) match {
    case Hit(script) => outMessageSink.send(LoadResponseMessage(repo, script))
    case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not load $name: ${snag.message}"))
  }

  def list(): Unit = outMessageSink.send(ListResponseMessage(repo.list))

  def save(name: String, content: String): Unit = repo.save(LoamScript(name, content, None)) match {
    case Hit(script) => outMessageSink.send(SaveResponseMessage(repo, script))
    case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not save $name: ${snag.message}"))
  }

  def unknownMessageType(inMessage: ClientInMessage): Unit =
    outMessageSink.send(ErrorOutMessage(s"Don't know what to do with incoming socket message '$inMessage'."))

  /** Handles messages sent in by a client */
  def handleInMessage(inMessage: ClientInMessage): Unit = {
    inMessage match {
      case CompileRequestMessage(code) => compile(code)
      case RunRequestMessage(code) => run(code)
      case LoadRequestMessage(name) => load(name)
      case ListRequestMessage => list()
      case SaveRequestMessage(name, content) => save(name, content)
      case _ => unknownMessageType(inMessage)
    }
  }
}

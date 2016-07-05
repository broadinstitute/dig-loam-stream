package loamstream.compiler.messages

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.util.{Hit, Miss}

import scala.concurrent.ExecutionContext

/** The handler responding to messages sent by a client */
object ClientMessageHandler {

  /** A receiver of messages sent to a client */
  object OutMessageSink {

    /** A receiver of messages that does nothing */
    object NoOp extends OutMessageSink {
      override def send(outMessage: ClientOutMessage): Unit = ()
    }

  }

  /** A receiver of messages sent to a client */
  trait OutMessageSink {
    /** Accepts messages to be sent to the client */
    def send(outMessage: ClientOutMessage)
  }

}

/** The handler responding to messages sent by a client */
case class ClientMessageHandler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext) {
  val repo = LoamRepository.defaultRepo
  val compiler = new LoamCompiler(outMessageSink)

  /** Handles messages sent in by a client */
  def handleInMessage(inMessage: ClientInMessage): Unit = {
    inMessage match {
      case CompileRequestMessage(text) =>
        outMessageSink.send(ReceiptOutMessage(text))
        compiler.compile(text)
      case LoadRequestMessage(name) =>
        repo.load(name) match {
          case Hit(loadResponseMessage) => outMessageSink.send(loadResponseMessage)
          case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not load $name: ${snag.message}"))
        }
      case ListRequestMessage =>
        outMessageSink.send(ListResponseMessage(repo.list))
      case SaveRequestMessage(name, content) =>
        repo.save(name, content) match {
          case Hit(saveResponseMessage) => outMessageSink.send(saveResponseMessage)
          case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not save $name: ${snag.message}"))
        }
      case _ =>
        outMessageSink.send(ErrorOutMessage(s"Don't know what to do with incoming socket message '$inMessage'."))
    }
  }
}

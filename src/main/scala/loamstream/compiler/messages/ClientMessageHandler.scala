package loamstream.compiler.messages

import loamstream.compiler.LoamCompiler
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink
import loamstream.compiler.repo.LoamRepository
import loamstream.util.{Hit, Miss}

import scala.concurrent.ExecutionContext

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object ClientMessageHandler {

  object OutMessageSink {

    object NoOp extends OutMessageSink {
      override def send(outMessage: ClientOutMessage): Unit = ()
    }

  }

  trait OutMessageSink {
    def send(outMessage: ClientOutMessage)
  }

}

case class ClientMessageHandler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext) {
  val repo = LoamRepository.defaultRepo
  val compiler = new LoamCompiler(outMessageSink)

  def handleInMessage(inMessage: ClientInMessage): Unit = {
    inMessage match {
      case TextSubmitMessage(text) =>
        outMessageSink.send(ReceiptOutMessage(text))
        compiler.compile(text)
      case LoadRequestMessage(name) =>
        repo.get(name) match {
          case Hit(content) => outMessageSink.send(LoadResponseMessage(name, content))
          case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not load $name: ${snag.message}"))
        }
      case ListRequestMessage =>
        outMessageSink.send(ListResponseMessage(repo.entries))
      case SaveRequestMessage(name, content) =>
        repo.add(name, content) match {
          case Hit(nameSaved) => outMessageSink.send(SaveResponseMessage(nameSaved))
          case Miss(snag) => outMessageSink.send(ErrorOutMessage(s"Could not save $name: ${snag.message}"))
        }
      case _ =>
        outMessageSink.send(ErrorOutMessage(s"Don't know what to do with incoming socket message '$inMessage'."))
    }
  }
}

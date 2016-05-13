package loamstream.web.parser

import loamstream.web.controllers.socket.SocketMessageHandler.OutMessageSink
import loamstream.web.controllers.socket.{ErrorOutMessage, ReceiptOutMessage, SocketInMessage, SocketMessageHandler,
TextSubmitMessage}

import scala.concurrent.ExecutionContext

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
case class ClientHandler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext)
  extends SocketMessageHandler {
  val compiler = new LoamCompiler(outMessageSink)

  override def handleInMessage(inMessage: SocketInMessage): Unit = {
    inMessage match {
      case TextSubmitMessage(text) =>
        outMessageSink.send(ReceiptOutMessage(text))
        compiler.compile(text)
      case _ =>
        outMessageSink.send(ErrorOutMessage(s"Don't know what to do with incoming socket message '$inMessage'."))
    }
  }
}

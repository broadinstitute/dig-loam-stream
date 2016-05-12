package loamstream.web.parser

import loamstream.web.controllers.socket.SocketMessageHandler.OutMessageSink
import loamstream.web.controllers.socket.{ErrorOutMessage, ReceiptOutMessage, SocketInMessage, SocketMessageHandler,
TextSubmitMessage}

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
case class ClientHandler(outMessageSink: OutMessageSink) extends SocketMessageHandler {
  override def handleInMessage(inMessage: SocketInMessage): Unit = {
    inMessage match {
      case TextSubmitMessage(text) =>
        outMessageSink.send(ReceiptOutMessage(text))
        val compiler = new LoamCompiler(outMessageSink)
        compiler.compile(text)
      case _ =>
        outMessageSink.send(ErrorOutMessage(s"Don't know what to do with incoming socket message '$inMessage'."))
    }
  }
}

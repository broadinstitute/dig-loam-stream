package loamstream.compiler

import loamstream.compiler.ClientMessageHandler.OutMessageSink

import scala.concurrent.ExecutionContext

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object ClientMessageHandler {

  trait OutMessageSink {
    def send(outMessage: ClientOutMessage)
  }

}

case class ClientMessageHandler(outMessageSink: OutMessageSink)(implicit executionContext: ExecutionContext) {
  val compiler = new LoamCompiler(outMessageSink)

  def handleInMessage(inMessage: ClientInMessage): Unit = {
    inMessage match {
      case TextSubmitMessage(text) =>
        outMessageSink.send(ReceiptOutMessage(text))
        compiler.compile(text)
      case _ =>
        outMessageSink.send(ErrorOutMessage(s"Don't know what to do with incoming socket message '$inMessage'."))
    }
  }
}

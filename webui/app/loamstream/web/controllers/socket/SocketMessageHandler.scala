package loamstream.web.controllers.socket

import loamstream.web.controllers.socket.SocketMessageHandler.OutMessageSink

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
object SocketMessageHandler {

  trait OutMessageSink {
    def send(outMessage: SocketOutMessage)
  }

}

trait SocketMessageHandler {

  def handleInMessage(inMessage: SocketInMessage, outMessageSink : OutMessageSink)

}

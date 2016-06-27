package loamstream.compiler.messages

/**
  * LoamStream
  * Created by oliverr on 5/9/2016.
  */
trait ClientOutMessage {

  def typeName: String
  def message: String

}

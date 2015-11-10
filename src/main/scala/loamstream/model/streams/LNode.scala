package loamstream.model.streams

/**
  * LoamStream
  * Created by oliverr on 11/10/2015.
  */
trait LNode {
  type Tag

  def id: String

  def tag: Tag
}

package loamstream.model

/**
  * @author Clint
  * @author Oliver
  *         date: Apr 26, 2016
  */
trait Tool extends LId.Owner {
  def inputs: Map[LId, Store]

  def outputs: Map[LId, Store]
}

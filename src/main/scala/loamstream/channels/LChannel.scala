package loamstream.channels

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 3/29/16.
  */
trait LChannel[-T, +R] extends ((T) => R) {

  def key: String

}

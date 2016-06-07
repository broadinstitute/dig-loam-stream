package loamstream.model

/**
  * LoamStream
  * Created by oliverr on 2/26/2016.
  */
final case class StoreSpec(sig: LSig) {
  def =:=(other: StoreSpec): Boolean = sig.tpe =:= other.sig.tpe

  def <:<(other: StoreSpec): Boolean = sig.tpe =:= other.sig.tpe

  def >:>(other: StoreSpec): Boolean = sig.tpe =:= other.sig.tpe
}

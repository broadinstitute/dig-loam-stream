package loamstream.loam.ops

/**
  * LoamStream
  * Created by oliverr on 11/16/2016.
  */
trait TextStore extends StoreType {
  override type Record <: TextStoreRecord
}

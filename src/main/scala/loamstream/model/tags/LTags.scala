package loamstream.model.tags

import util.Index

/**
  * LoamStream
  * Created by oliverr on 1/13/2016.
  */
object LTags {

  trait IsMap extends LTags

  trait IsSet extends LTags

  trait HasV[V] extends IsMap

  trait HasKey[I <: Index, K] extends LTags

}

trait LTags {

}

package loamstream.model.kinds.instances

import loamstream.model.kinds.LNamedKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object RecipeKinds {
  val usePreExisting = LNamedKind("Use pre-existing")
  val extractFirstKey = LNamedKind("Extract first key")
  val extractSampleIdsFromGenotypeCalls = LNamedKind("Extract sample ids from genotype calls.")
}

package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind

/**
  * LoamStream
  * Created by oliverr on 2/16/2016.
  */
object RecipeKinds {
  val usePreExisting = LSpecificKind("Use pre-existing")
  val extractFirstKey = LSpecificKind("Extract first key")
  val extractSampleIdsFromGenotypeCalls = LSpecificKind("Extract sample ids from genotype calls.")
}

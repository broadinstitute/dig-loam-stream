package loamstream.model.kinds.instances

import loamstream.model.kinds.LSpecificKind

/**
 * LoamStream
 * Created by oliverr on 2/16/2016.
 */
object RecipeKinds {
  def usePreExisting(id: String) = LSpecificKind(("Use pre-existing", id))

  def extractKey(index: Int) = LSpecificKind(("Extract key", index))

  val extractSampleIdsFromGenotypeCalls = LSpecificKind("Extract sample ids from genotype calls.")
}

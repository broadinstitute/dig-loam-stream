package loamstream.model.recipes

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LRecipe[Inputs <: LPileCalls[_, _, _]] {
  def inputs: Inputs
}

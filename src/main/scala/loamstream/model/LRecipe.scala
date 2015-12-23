package loamstream.model

import loamstream.model.tags.BList

import scala.language.higherKinds

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
trait LRecipe {
  type EU
  type E
  type B[_]
  type T <: BList[EU, _, B, _]
  type Inputs <: BList[EU, E, B, T]

  def inputs: Inputs
}

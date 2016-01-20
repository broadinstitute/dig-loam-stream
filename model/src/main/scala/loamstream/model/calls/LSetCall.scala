package loamstream.model.calls

import loamstream.model.recipes.{LCheckoutPreexisting, LRecipe}
import util.ProductTypeExploder
import util.shot.Shot

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LSetCall {
  def getPreexisting[Keys <: Product : TypeTag](id: String): Shot[LSetCall]
  = apply[Keys](new LCheckoutPreexisting(id))

  def apply[Keys <: Product : TypeTag](recipe: LRecipe): Shot[LSetCall] =
    ProductTypeExploder.explode(typeTag[Keys].tpe).map(LSetCall(_, recipe))
}

case class LSetCall(keyTypes: Seq[Type], recipe: LRecipe) extends LPileCall {
}

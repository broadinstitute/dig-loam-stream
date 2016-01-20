package loamstream.model.calls

import loamstream.model.recipes.{LCheckoutPreexisting, LPileCalls, LRecipe}
import util.ProductTypeExploder
import util.shot.Shot

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LSetCall {
  def getPreexisting[Keys <: Product : TypeTag](id: String): Shot[LSetCall[Keys, LPileCalls.LCalls0]]
  = apply[Keys, LPileCalls.LCalls0](new LCheckoutPreexisting(id))

  def apply[Keys <: Product : TypeTag, Inputs <: LPileCalls[_, _]](recipe: LRecipe[Inputs]): Shot[LSetCall[Keys, Inputs]] =
    ProductTypeExploder.explode(typeTag[Keys].tpe).map(LSetCall(_, recipe))
}

case class LSetCall[Keys <: Product, Inputs <: LPileCalls[_, _]](keyTypes: Seq[Type], recipe: LRecipe[Inputs])
  extends LPileCall[Keys, Inputs] {
}

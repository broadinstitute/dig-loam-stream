package loamstream.model.calls

import loamstream.model.recipes.LRecipe
import util.ProductTypeExploder
import util.shot.Shot

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LMapCall {

  //  def getPreexisting[Tag <: LMapTag[_, _, _], Props <: LProps](tag: Tag, id: String):
  //  LMapCall[Tag, LPileCalls.LCalls0, Props]
  //  = LMapCall(tag, new LCheckoutPreexisting(id))
  def apply[Keys <: Product : TypeTag, V: TypeTag](recipe: LRecipe): Shot[LMapCall[V]] =
    ProductTypeExploder.explode(typeTag[Keys].tpe).map(LMapCall(_, typeTag[V].tpe, recipe))
}

case class LMapCall[V: TypeTag](keyTypes: Seq[Type], vType: Type, recipe: LRecipe)
  extends LPileCall {
}

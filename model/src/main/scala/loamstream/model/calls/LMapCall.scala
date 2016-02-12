package loamstream.model.calls

import loamstream.model.kinds.LKind
import loamstream.model.recipes.LRecipe
import util.ProductTypeExploder
import util.shot.Shot

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 12/23/2015.
  */
object LMapCall {

  def apply[Keys <: Product : TypeTag, V: TypeTag](recipe: LRecipe, kind: LKind): Shot[LMapCall[V]] =
    ProductTypeExploder.explode(typeTag[Keys].tpe).map(LMapCall(_, typeTag[V].tpe, recipe, kind))
}

case class LMapCall[V: TypeTag](keyTypes: Seq[Type], vType: Type, recipe: LRecipe, kind: LKind)
  extends LPileCall {
}

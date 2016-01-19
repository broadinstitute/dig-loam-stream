package loamstream.kit

import loamstream.model.calls.LPileCall
import loamstream.model.recipes.LPileCalls
import util.Index

import scala.reflect.macros.whitebox.Context

/**
  * LoamStream
  * Created by oliverr on 1/19/2016.
  */
object LKitMacrosImpl {

  def extractKey[I <: Index : context.WeakTypeTag, Keys <: Product : context.WeakTypeTag,
  Inputs <: LPileCalls[_, _] : context.WeakTypeTag](context: Context)(pile: context.Expr[LPileCall[Keys, Inputs]]):
  context.Expr[AnyRef] = {
    println("Yo macro!")
    pile
  }

}

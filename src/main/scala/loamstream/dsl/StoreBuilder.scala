package loamstream.dsl

import loamstream.model.LId

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object StoreBuilder {
  def create[T: TypeTag](implicit flowBuilder: FlowBuilder): StoreBuilder =
    StoreBuilder(LId.newAnonId, typeTag[T].tpe)
}

case class StoreBuilder(id: LId, tpe: Type)(implicit flowBuilder: FlowBuilder) {
  update()

  def update(): Unit = flowBuilder.add(this)

  override def toString: String = tpe.toString
}


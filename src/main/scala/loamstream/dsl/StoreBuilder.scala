package loamstream.dsl

import loamstream.model.LId

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object StoreBuilder {
  def defaultIsInput = false

  def create[T: TypeTag](implicit flowBuilder: FlowBuilder): StoreBuilder =
    StoreBuilder(LId.newAnonId, typeTag[T].tpe, defaultIsInput)
}

case class StoreBuilder(id: LId, tpe: Type, isInput: Boolean)(implicit flowBuilder: FlowBuilder) {
  update()

  def update(): Unit = flowBuilder.add(this)

  def asInput = {
    val newThis = copy(isInput = true)
    newThis.update()
    newThis
  }

  override def toString: String = if (isInput) s"in[$tpe]" else s"out[$tpe]"
}


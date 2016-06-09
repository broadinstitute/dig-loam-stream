package loamstream.dsl

import java.nio.file.Path

import loamstream.dsl.FlowBuilder.StoreSource
import loamstream.model.LId

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object StoreBuilder {
  def create[T: TypeTag](implicit flowBuilder: FlowBuilder): StoreBuilder =
    StoreBuilder(LId.newAnonId, typeTag[T].tpe, None)
}

case class StoreBuilder(id: LId, tpe: Type, sourceOpt: Option[StoreSource])(implicit flowBuilder: FlowBuilder) {
  update()

  def update(): Unit = flowBuilder.add(this)

  def from(path: Path): StoreBuilder = from(StoreSource.FromPath(path))

  def from(source: StoreSource): StoreBuilder = {
    flowBuilder.addSource(this, source)
    this
  }

  override def toString: String = sourceOpt match {
    case Some(StoreSource.FromPath(path)) => s"store[$tpe]@$path"
    case Some(source) => s"store[$tpe]@$source"
    case None => s"store[$tpe]"
  }
}


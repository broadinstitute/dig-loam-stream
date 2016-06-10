package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreSource
import loamstream.model.LId

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object StoreBuilder {
  def create[T: TypeTag](implicit graphBuilder: LoamGraphBuilder): StoreBuilder =
    StoreBuilder(LId.newAnonId, typeTag[T].tpe, None)
}

case class StoreBuilder(id: LId, tpe: Type, sourceOpt: Option[StoreSource])(implicit graphBuilder: LoamGraphBuilder) {
  update()

  def update(): Unit = graphBuilder.add(this)

  def from(path: Path): StoreBuilder = from(StoreSource.FromPath(path))

  def from(key: LEnv.Key[Path]): StoreBuilder = from(StoreSource.FromPathKey(key))

  def from(source: StoreSource): StoreBuilder = {
    graphBuilder.addSource(this, source)
    this
  }

  override def toString: String = sourceOpt match {
    case Some(StoreSource.FromPath(path)) => s"store[$tpe]@$path"
    case Some(source) => s"store[$tpe]@$source"
    case None => s"store[$tpe]"
  }
}


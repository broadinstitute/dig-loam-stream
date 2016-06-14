package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.model.LId

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object StoreBuilder {
  def create[T: TypeTag](implicit graphBuilder: LoamGraphBuilder): StoreBuilder =
    StoreBuilder(LId.newAnonId, typeTag[T].tpe)
}

case class StoreBuilder(id: LId, tpe: Type)(implicit graphBuilder: LoamGraphBuilder) {
  update()

  def update(): Unit = graphBuilder.add(this)

  def from(path: Path): StoreBuilder = from(StoreEdge.PathEdge(path))

  def from(key: LEnv.Key[Path]): StoreBuilder = from(StoreEdge.PathKeyEdge(key))

  def from(source: StoreEdge): StoreBuilder = {
    graphBuilder.addSource(this, source)
    this
  }

  def to(path: Path): StoreBuilder = to(StoreEdge.PathEdge(path))

  def to(key: LEnv.Key[Path]): StoreBuilder = to(StoreEdge.PathKeyEdge(key))

  def to(sink: StoreEdge): StoreBuilder = {
    graphBuilder.addSink(this, sink)
    this
  }

}


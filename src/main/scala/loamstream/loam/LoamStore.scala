package loamstream.loam

import java.nio.file.Path

import loamstream.LEnv
import loamstream.loam.LoamGraph.StoreEdge
import loamstream.model.{LId, Store, StoreSpec}

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object LoamStore {
  def create[T: TypeTag](implicit graphBuilder: LoamGraphBuilder): LoamStore =
    LoamStore(LId.newAnonId, typeTag[T].tpe)
}

case class LoamStore private(id: LId, tpe: Type)(implicit graphBuilder: LoamGraphBuilder) extends Store {
  update()

  def update(): Unit = graphBuilder.addStore(this)

  def from(path: Path): LoamStore = from(StoreEdge.PathEdge(path))

  def from(key: LEnv.Key[Path]): LoamStore = from(StoreEdge.PathKeyEdge(key))

  def from(source: StoreEdge): LoamStore = {
    graphBuilder.addSource(this, source)
    this
  }

  def to(path: Path): LoamStore = to(StoreEdge.PathEdge(path))

  def to(key: LEnv.Key[Path]): LoamStore = to(StoreEdge.PathKeyEdge(key))

  def to(sink: StoreEdge): LoamStore = {
    graphBuilder.addSink(this, sink)
    this
  }

  override def toString: String = s"store[$tpe]"

  def graph: LoamGraph = graphBuilder.graph

  override val spec: StoreSpec = StoreSpec(tpe)
}


package loamstream.dsl

import java.nio.file.Path

import loamstream.LEnv
import loamstream.dsl.StoreBuilder.{Source, SourcePath}
import loamstream.model.LId

import scala.reflect.runtime.universe.{Type, TypeTag, typeTag}

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object StoreBuilder {

  trait Source

  case class SourcePath(path: Path) extends Source

  case class SourcePathKey(key: LEnv.Key[Path]) extends Source

  case class SourcePathSourceKey(key: LEnv.Key[() => Path]) extends Source

  def create[T: TypeTag](implicit flowBuilder: FlowBuilder): StoreBuilder =
    StoreBuilder(LId.newAnonId, typeTag[T].tpe, None)
}

case class StoreBuilder(id: LId, tpe: Type, sourceOpt: Option[Source])(implicit flowBuilder: FlowBuilder) {
  update()

  def update(): Unit = flowBuilder.add(this)

  def from(path: Path): StoreBuilder = from(SourcePath(path))

  def from(provider: Source): StoreBuilder = {
    val newStore = copy(sourceOpt = Some(provider))
    newStore.update()
    newStore
  }

  override def toString: String = sourceOpt match {
    case Some(SourcePath(path)) => s"store[$tpe]@$path"
    case Some(source) => s"store[$tpe]@$source"
    case None => s"store[$tpe]"
  }
}


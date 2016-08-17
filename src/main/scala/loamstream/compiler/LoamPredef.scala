package loamstream.compiler

import java.nio.file.{Files, Path, Paths}

import htsjdk.variant.variantcontext.Genotype
import loamstream.loam.LoamTool.DefaultStores
import loamstream.loam._

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/** Predefined symbols in Loam scripts */
object LoamPredef {

  implicit def toConstantFunction[T](item: T): () => T = () => item

  def path(pathString: String): Path = Paths.get(pathString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store[T: TypeTag](implicit context: LoamContext): LoamStore = LoamStore.create[T]

  def job[T: TypeTag](exp: => T)(implicit context: LoamContext): LoamNativeTool[T] =
    LoamNativeTool(DefaultStores.empty, exp)

  def job[T: TypeTag](store: LoamStore, stores: LoamStore*)(exp: => T)(
    implicit context: LoamContext): LoamNativeTool[T] =
    LoamNativeTool((store +: stores).toSet, exp)

  def in(store: LoamStore, stores: LoamStore*): LoamTool.In = in(store +: stores)

  def in(stores: Iterable[LoamStore]): LoamTool.In = LoamTool.In(stores)

  def out(store: LoamStore, stores: LoamStore*): LoamTool.Out = LoamTool.Out((store +: stores).toSet)

  def out(stores: Iterable[LoamStore]): LoamTool.Out = LoamTool.Out(stores)

  def job[T: TypeTag](in: LoamTool.In, out: LoamTool.Out)(exp: => T)(
    implicit context: LoamContext): LoamNativeTool[T] = LoamNativeTool(in, out, exp)

  def job[T: TypeTag](in: LoamTool.In)(exp: => T)(
    implicit context: LoamContext): LoamNativeTool[T] = LoamNativeTool(in, exp)

  def job[T: TypeTag](out: LoamTool.Out)(exp: => T)(
    implicit context: LoamContext): LoamNativeTool[T] = LoamNativeTool(out, exp)

  def thisTool(implicit context: LoamContext): LoamNativeTool[_] = context.getCurrentTool.get

  trait VCF extends Map[(String, String), Genotype]

  trait TXT extends Seq[String]

}

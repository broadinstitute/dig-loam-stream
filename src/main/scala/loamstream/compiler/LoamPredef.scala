package loamstream.compiler

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import htsjdk.variant.variantcontext.Genotype
import loamstream.loam.LoamTool.DefaultStores
import loamstream.loam._
import loamstream.loam.ops.{StoreRecord, TextStore, TextStoreRecord}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/** Predefined symbols in Loam scripts */
object LoamPredef {

  implicit def toConstantFunction[T](item: T): () => T = () => item

  def path(pathString: String): Path = Paths.get(pathString)

  def uri(uriString: String): URI = URI.create(uriString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store[T: TypeTag](implicit scriptContext: LoamScriptContext): LoamStore[T] = LoamStore.create[T]

  def job[T: TypeTag](exp: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] =
    LoamNativeTool(DefaultStores.empty, exp)

  def job[T: TypeTag](store: LoamStore.Untyped, stores: LoamStore.Untyped*)(exp: => T)(
    implicit scriptContext: LoamScriptContext): LoamNativeTool[T] =
    LoamNativeTool((store +: stores).toSet, exp)

  def in(store: LoamStore.Untyped, stores: LoamStore.Untyped*): LoamTool.In = in(store +: stores)

  def in(stores: Iterable[LoamStore.Untyped]): LoamTool.In = LoamTool.In(stores)

  def out(store: LoamStore.Untyped, stores: LoamStore.Untyped*): LoamTool.Out = LoamTool.Out((store +: stores).toSet)

  def out(stores: Iterable[LoamStore.Untyped]): LoamTool.Out = LoamTool.Out(stores)

  def job[T: TypeTag](in: LoamTool.In, out: LoamTool.Out)(exp: => T)(
    implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = LoamNativeTool(in, out, exp)

  def job[T: TypeTag](in: LoamTool.In)(exp: => T)(
    implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = LoamNativeTool(in, exp)

  def job[T: TypeTag](out: LoamTool.Out)(exp: => T)(
    implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = LoamNativeTool(out, exp)

  def changeDir(newPath: Path)(implicit scriptContext: LoamScriptContext): Path = scriptContext.changeWorkDir(newPath)

  def changeDir(newPath: String)(implicit scriptContext: LoamScriptContext): Path = changeDir(Paths.get(newPath))

  def inDir[T](path: Path)(expr: => T)(implicit scriptContext: LoamScriptContext): T = {
    val oldDir = scriptContext.workDir
    scriptContext.changeWorkDir(path)
    val value = expr
    scriptContext.setWorkDir(oldDir)
    value
  }

  def inDir[T](path: String)(expr: => T)(implicit scriptContext: LoamScriptContext): T =
    inDir[T](Paths.get(path))(expr)

  trait VCF extends TextStore

  trait TXT extends TextStore

}

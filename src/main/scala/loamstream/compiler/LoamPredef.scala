package loamstream.compiler

import java.nio.file.{Files, Path, Paths}

import htsjdk.variant.variantcontext.Genotype
import loamstream.loam.LoamTool.DefaultStores
import loamstream.loam.{LoamGraph, LoamNativeTool, LoamStore, LoamTool}
import loamstream.util.ValueBox

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/** Predefined symbols in Loam scripts */
object LoamPredef {

  implicit def toConstantFunction[T](item: T): () => T = () => item

  def path(pathString: String): Path = Paths.get(pathString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store[T: TypeTag](implicit graphBox: ValueBox[LoamGraph]): LoamStore = LoamStore.create[T]

  def job[T: TypeTag](exp: => T)(implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] =
    LoamNativeTool(DefaultStores.empty, exp)

  def job[T: TypeTag](store: LoamStore, stores: LoamStore*)(exp: => T)(
    implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] =
    LoamNativeTool((store +: stores).toSet, exp)

  def in(store: LoamStore, stores: LoamStore*): LoamTool.In = LoamTool.In((store +: stores).toSet)

  def out(store: LoamStore, stores: LoamStore*): LoamTool.Out = LoamTool.Out((store +: stores).toSet)

  def job[T: TypeTag](in: LoamTool.In, out: LoamTool.Out)(exp: => T)(
    implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] = LoamNativeTool(in, out, exp)

  def job[T: TypeTag](in: LoamTool.In)(exp: => T)(
    implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] = LoamNativeTool(in, exp)

  def job[T: TypeTag](out: LoamTool.Out)(exp: => T)(
    implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] = LoamNativeTool(out, exp)

  trait VCF extends Map[(String, String), Genotype]

  trait TXT extends Seq[String]

}

package loamstream.compiler

import java.nio.file.{Files, Path, Paths}

import htsjdk.variant.variantcontext.Genotype
import loamstream.loam.{LoamGraph, LoamNativeTool, LoamStore}
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

  def job[T](exp: => T)(implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] = LoamNativeTool(Set.empty, exp)

  def job[T](store: LoamStore, stores: LoamStore*)(exp: => T)(
    implicit graphBox: ValueBox[LoamGraph]): LoamNativeTool[T] =
    LoamNativeTool((store +: stores).toSet, exp)

  trait VCF extends Map[(String, String), Genotype]

  trait TXT extends Seq[String]

}

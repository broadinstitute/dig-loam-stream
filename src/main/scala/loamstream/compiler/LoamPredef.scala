package loamstream.compiler

import java.nio.file.{Files, Path, Paths}

import htsjdk.variant.variantcontext.Genotype
import loamstream.LEnv.Key
import loamstream.loam.{LoamGraph, LoamStore}
import loamstream.util.ValueBox

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object LoamPredef {

  implicit def toConstantFunction[T](item: T): () => T = () => item

  def path(pathString: String): Path = Paths.get(pathString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def key[T: TypeTag]: Key[T] = Key.create[T]

  def store[T: TypeTag](implicit graphBox: ValueBox[LoamGraph]): LoamStore = LoamStore.create[T]

  trait VCF extends Map[(String, String), Genotype]
  trait TXT extends  Seq[String]

}

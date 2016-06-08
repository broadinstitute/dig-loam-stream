package loamstream.compiler

import java.nio.file.{Path, Paths}

import htsjdk.variant.variantcontext.Genotype
import loamstream.LEnv.Key
import loamstream.dsl.{InputBuilder, OutputBuilder}
import loamstream.tools.core.LCoreEnv

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object LoamPredef {

  implicit def toConstantFunction[T](item: T): () => T = () => item

  def path(pathString: String): Path = Paths.get(pathString)

  def tempFile(prefix: String, suffix: String): () => Path = LCoreEnv.tempFileProvider(prefix, suffix)

  def tempDir(prefix: String): () => Path = LCoreEnv.tempDirProvider(prefix)

  def key[T: TypeTag]: Key[T] = Key.create[T]

  def in[T: TypeTag](name: String): InputBuilder = InputBuilder[T](name)

  def out[T: TypeTag](name: String): OutputBuilder = OutputBuilder[T](name)

  type VCF = Map[(String, String), Genotype]

}

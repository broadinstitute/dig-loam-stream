package loamstream.compiler

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import loamstream.loam.LoamTool.DefaultStores
import loamstream.loam._
import loamstream.loam.ops.StoreType

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import loamstream.model.execute.ExecutionEnvironment
import loamstream.conf.{DataConfig, DynamicConfig}
import loamstream.util.ConfigUtils

/** Predefined symbols in Loam scripts */
object LoamPredef {

  implicit def toConstantFunction[T](item: T): () => T = () => item
  
  def path(pathString: String): Path = Paths.get(pathString)

  def uri(uriString: String): URI = URI.create(uriString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store[S <: StoreType : TypeTag](implicit scriptContext: LoamScriptContext): LoamStore[S] =
    LoamStore.create[S]

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
    
    try {
      scriptContext.changeWorkDir(path)
      expr
    } finally {
      scriptContext.setWorkDir(oldDir)
    }
  }

  def inDir[T](path: String)(expr: => T)(implicit scriptContext: LoamScriptContext): T =
    inDir[T](Paths.get(path))(expr)

  private[this] def runIn[A](env: ExecutionEnvironment)(expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    val oldEnv = scriptContext.executionEnvironment
    
    try {
      scriptContext.executionEnvironment = env
      expr
    } finally {
      scriptContext.executionEnvironment = oldEnv
    }
  }
  
  def local[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    runIn(ExecutionEnvironment.Local)(expr)(scriptContext)
  }
  
  def uger[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    runIn(ExecutionEnvironment.Uger)(expr)(scriptContext)
  }
  
  def google[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    runIn(ExecutionEnvironment.Google)(expr)(scriptContext)
  }
  
  def loadConfig(path: String): DynamicConfig = {
    val config = ConfigUtils.configFromFile(Paths.get(path))
    
    DynamicConfig(config)
  }

  def loadDataConfig(path: String): DataConfig = DataConfig.fromFile(path)
}

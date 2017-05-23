package loamstream.compiler

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import loamstream.model.Tool.DefaultStores
import loamstream.loam._
import loamstream.loam.ops.StoreType

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import loamstream.model.execute.ExecutionEnvironment
import loamstream.conf.{DataConfig, DynamicConfig}
import loamstream.model.{Store, Tool}
import loamstream.util.ConfigUtils

/** Predefined symbols in Loam scripts */
object LoamPredef {

  implicit def toConstantFunction[T](item: T): () => T = () => item
  
  def path(pathString: String): Path = Paths.get(pathString)

  def uri(uriString: String): URI = URI.create(uriString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store[S <: StoreType : TypeTag](implicit scriptContext: LoamScriptContext): Store[S] = Store.create[S]

  def job[T: TypeTag](exp: => T)(implicit scriptContext: LoamScriptContext): LoamNativeTool[T] =
    LoamNativeTool(DefaultStores.empty, exp)

  def job[T: TypeTag](store: Store.Untyped, stores: Store.Untyped*)(exp: => T)(
    implicit scriptContext: LoamScriptContext): LoamNativeTool[T] =
    LoamNativeTool((store +: stores).toSet, exp)

  def in(store: Store.Untyped, stores: Store.Untyped*): Tool.In = in(store +: stores)

  def in(stores: Iterable[Store.Untyped]): Tool.In = Tool.In(stores)

  def out(store: Store.Untyped, stores: Store.Untyped*): Tool.Out = Tool.Out((store +: stores).toSet)

  def out(stores: Iterable[Store.Untyped]): Tool.Out = Tool.Out(stores)

  def job[T: TypeTag](in: Tool.In, out: Tool.Out)(exp: => T)(
    implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = LoamNativeTool(in, out, exp)

  def job[T: TypeTag](in: Tool.In)(exp: => T)(
    implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = LoamNativeTool(in, exp)

  def job[T: TypeTag](out: Tool.Out)(exp: => T)(
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

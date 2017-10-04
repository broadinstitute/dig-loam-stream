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
import loamstream.util.Loggable

/** Predefined symbols in Loam scripts */
object LoamPredef extends Loggable {

  type Store[A <: StoreType] = loamstream.model.Store[A]
  
  implicit def toConstantFunction[T](item: T): () => T = () => item
  
  def path(pathString: String): Path = Paths.get(pathString)

  def uri(uriString: String): URI = URI.create(uriString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store[S <: StoreType : TypeTag](implicit context: LoamScriptContext): Store[S] = {
    Store.create[S]
  }

  def job[T: TypeTag](exp: => T)
                     (implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {
    LoamNativeTool(DefaultStores.empty, exp)
  }

  def job[T: TypeTag](store: Store.Untyped, stores: Store.Untyped*)
                     (exp: => T)
                     (implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {
    LoamNativeTool((store +: stores).toSet, exp)
  }

  def job[T: TypeTag](in: Tool.In, out: Tool.Out)
                     (exp: => T)
                     (implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {
    LoamNativeTool(in, out, exp)
  }

  def job[T: TypeTag](in: Tool.In)
                     (exp: => T)
                     (implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {
    LoamNativeTool(in, exp)
  }

  def job[T: TypeTag](out: Tool.Out)
                     (exp: => T)
                     (implicit scriptContext: LoamScriptContext): LoamNativeTool[T] = {
    LoamNativeTool(out, exp)
  }

  /**
   * Indicate that jobs derived from tools/stores created by `loamCode` should run
   * AFTER jobs derived from tools/stores defined BEFORE the `andThen`, in evaluation order.
   * Enables code like:
   * 
   *  val in = store[TXT].at("input-file").asInput
   *  val computed = store[TXT].at("computed")
   *  
   *  cmd"compute-something -i $in -o $computed".in(in).out(computed)
   *  
   *  andThen {
   *    //needs to wait for 'compute-something' command to finish
   *    val n = countLinesIn(computed) 
   *    
   *    for(i <- 1 to n) {
   *      val fooOut = store[TXT].at(s"foo-out-$i.txt")
   *    
   *      cmd"foo -i $computed -n $i".in(computed).out(fooOut)
   *    }
   *  }
   */
  def andThen(loamCode: => Any)(implicit scriptContext: LoamScriptContext): Unit = {
    // TODO: try-catch to print a friendlier message in case 'loamCode' throws exception
    // TODO: makes sense to asynchronously evaluate 'loamCode'?
    
    scriptContext.projectContext.registerGraphSoFar()
    
    scriptContext.projectContext.registerLoamThunk(loamCode)
  }
  
  def in(store: Store.Untyped, stores: Store.Untyped*): Tool.In = in(store +: stores)

  def in(stores: Iterable[Store.Untyped]): Tool.In = Tool.In(stores)

  def out(store: Store.Untyped, stores: Store.Untyped*): Tool.Out = Tool.Out((store +: stores).toSet)

  def out(stores: Iterable[Store.Untyped]): Tool.Out = Tool.Out(stores)

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

  /**
   * Parse the config file into a DataConfig.
   * @param path a string representing the path of the file to parse.
   */
  def loadConfig(path: String): DataConfig = DataConfig.fromFile(path)
  
  /**
   * Look for a system property containing the path to the config file to parse.
   * If the key isn't found, use a default path.  In either case, parse the config
   * file at the path into a DataConfig.
   * @param key the name of a JVM system property to look for.  If the property is
   * defined, use its value as a path to a config file to parse into a DataConfig.
   * @param defaultPath a string representing the path of the file to parse if no
   * JVM system property can be found using the param `key`.
   */
  def loadConfig(key: String, defaultPath: String): DataConfig = {
    val pathToLoad = System.getProperty(key, defaultPath)
    
    debug(s"Loading config file: '${pathToLoad}'")
    
    loadConfig(pathToLoad)
  }
}

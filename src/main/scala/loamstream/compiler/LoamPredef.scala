package loamstream.compiler

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import com.google.protobuf.duration.Duration
import loamstream.model.Tool.DefaultStores
import loamstream.loam._
import loamstream.loam.ops.StoreType

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import loamstream.model.execute.{ExecutionEnvironment, Memory}
import loamstream.conf.{DataConfig, DynamicConfig}
import loamstream.model.{Store, Tool}
import loamstream.util.ConfigUtils

/** Predefined symbols in Loam scripts */
object LoamPredef {

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

  /**
   * @param mem Memory requested per job submission in Gb's
   * @param cores Number of cores requested per job submission
   * @param maxRunTime Time limit after which a job may get killed
   * @param expr Block of cmd's and native code
   * @param scriptContext Container for compile time and run time context for a script
   */
  def uger[A](mem: Double = 1, cores: Int = 1, maxRunTime: Double = 2)
             (expr: => A)
             (implicit scriptContext: LoamScriptContext): A = {
    import scala.concurrent.duration._

    runIn(ExecutionEnvironment.Uger(Memory.inGb(mem), cores, maxRunTime.hours))(expr)(scriptContext)
  }
  
  def google[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    runIn(ExecutionEnvironment.Google)(expr)(scriptContext)
  }

  def loadConfig(path: String): DataConfig = DataConfig.fromFile(path)
}

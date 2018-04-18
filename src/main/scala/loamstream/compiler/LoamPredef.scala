package loamstream.compiler

import java.net.URI
import java.nio.file.{Files, Path, Paths}

import loamstream.conf.DataConfig
import loamstream.loam.LoamScriptContext
import loamstream.model.{Store, Tool}
import loamstream.model.execute.{Environment, GoogleSettings, UgerSettings}
import loamstream.model.quantities.{CpuTime, Cpus, Memory}
import loamstream.util.Loggable

/** Predefined symbols in Loam scripts */
object LoamPredef extends Loggable {

  type Store = loamstream.model.Store
  
  def path(pathString: String): Path = Paths.get(pathString)

  def uri(uriString: String): URI = URI.create(uriString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store(implicit context: LoamScriptContext): Store = Store.create

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
  
  def in(store: Store, stores: Store*): Tool.In = in(store +: stores)

  def in(stores: Iterable[Store]): Tool.In = Tool.In(stores)

  def out(store: Store, stores: Store*): Tool.Out = Tool.Out((store +: stores).toSet)

  def out(stores: Iterable[Store]): Tool.Out = Tool.Out(stores)

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

  private[this] def runIn[A](env: Environment)(expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    val oldEnv = scriptContext.executionEnvironment
    
    try {
      scriptContext.executionEnvironment = env
      expr
    } finally {
      scriptContext.executionEnvironment = oldEnv
    }
  }
  
  def local[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    runIn(Environment.Local)(expr)(scriptContext)
  }

  def uger[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    val ugerConfig = scriptContext.ugerConfig 
    
    val settings = UgerSettings(ugerConfig.defaultCores, ugerConfig.defaultMemoryPerCore, ugerConfig.defaultMaxRunTime)
    
    val env = Environment.Uger(settings)
    
    runIn(env)(expr)(scriptContext)
  }
  
  /**
   * @param mem Memory requested per core, per job submission (in Gb's)
   * @param cores Number of cores requested per job submission
   * @param maxRunTime Time limit (in hours) after which a job may get killed
   * @param expr Block of cmd's and native code
   * @param scriptContext Container for compile time and run time context for a script
   */
  def ugerWith[A](
      cores: Int = -1,
      mem: Double = -1, 
      maxRunTime: Double = -1)
      (expr: => A)
      (implicit scriptContext: LoamScriptContext): A = {
    
    val ugerConfig = scriptContext.ugerConfig
    
    def orDefault[B](actual: B, default: B) = if(actual == -1) default else actual
    
    val settings = UgerSettings(
        Cpus(orDefault(cores, ugerConfig.defaultCores.value)), 
        Memory.inGb(orDefault(mem, ugerConfig.defaultMemoryPerCore.gb)), 
        CpuTime.inHours(orDefault(maxRunTime, ugerConfig.defaultMaxRunTime.hours)))
    
    val env = Environment.Uger(settings)
    
    runIn(env)(expr)(scriptContext)
  }
  
  def google[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    val settings = GoogleSettings(scriptContext.googleConfig.clusterId) 
    
    runIn(Environment.Google(settings))(expr)(scriptContext)
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
    
    debug(s"Loading config file: '$pathToLoad'")
    
    loadConfig(pathToLoad)
  }
}

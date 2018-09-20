package loamstream.compiler

import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

import loamstream.conf.DataConfig
import loamstream.conf.DrmConfig
import loamstream.drm.DockerParams
import loamstream.loam.LoamGraph.StoreLocation
import loamstream.loam.LoamScriptContext
import loamstream.model.Store
import loamstream.model.execute.Environment
import loamstream.model.execute.GoogleSettings
import loamstream.model.quantities.CpuTime
import loamstream.model.quantities.Cpus
import loamstream.model.quantities.Memory
import loamstream.util.Loggable

/** Predefined symbols in Loam scripts */
object LoamPredef extends Loggable {

  type Store = loamstream.model.Store
  
  def path(pathString: String): Path = Paths.get(pathString)

  def uri(uriString: String): URI = URI.create(uriString)

  def tempFile(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDir(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  def store(implicit context: LoamScriptContext): Store = Store()

  def store(path: String)(implicit context: LoamScriptContext): Store = store(Paths.get(path))

  def store(path: Path)(implicit context: LoamScriptContext): Store = {
    val resolvedPath = context.workDir.resolve(path)
    val location = StoreLocation.PathLocation(resolvedPath)
    
    store(location)
  }

  def store(uri: URI)(implicit context: LoamScriptContext): Store = {
    val location = StoreLocation.UriLocation(uri)
    
    store(location)
  }

  def store(location: StoreLocation)(implicit context: LoamScriptContext): Store = Store(location)
  
  /**
   * Indicate that jobs derived from tools/stores created by `loamCode` should run
   * AFTER jobs derived from tools/stores defined BEFORE the `andThen`, in evaluation order.
   * Enables code like:
   * 
   *  val in = store.at("input-file").asInput
   *  val computed = store.at("computed")
   *  
   *  cmd"compute-something -i $in -o $computed".in(in).out(computed)
   *  
   *  andThen {
   *    //needs to wait for 'compute-something' command to finish
   *    val n = countLinesIn(computed) 
   *    
   *    for(i <- 1 to n) {
   *      val fooOut = store.at(s"foo-out-$i.txt")
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
  
  def changeDir(newPath: Path)(implicit scriptContext: LoamScriptContext): Path = scriptContext.changeWorkDir(newPath)

  def changeDir(newPath: String)(implicit scriptContext: LoamScriptContext): Path = changeDir(Paths.get(newPath))

  def inDir[T](path: Path)(expr: => T)(implicit scriptContext: LoamScriptContext): T = {
    val oldDir = scriptContext.workDir
    
    try {
      changeDir(path)
      expr
    } finally {
      scriptContext.setWorkDir(oldDir)
    }
  }

  def inDir[T](path: String)(expr: => T)(implicit scriptContext: LoamScriptContext): T = {
    inDir[T](Paths.get(path))(expr)
  }

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

  @deprecated("uger { ... } blocks are deprecated; use drm { ... } instead.", "")
  def uger[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = drm(expr)(scriptContext)
  
  def drm[A](expr: => A)(implicit scriptContext: LoamScriptContext): A = {
    val loamConfig = scriptContext.config 
    
    require(
        loamConfig.drmSystem.isDefined, 
        "A DRM system (Uger or LSF) must be specified on the command-line to run jobs in a DRM environment")
    
    val env = loamConfig.drmSystem.get.makeBasicEnvironment(scriptContext)
    
    runIn(env)(expr)(scriptContext)
  }
  
  /**
   * @param mem Memory requested per core, per job submission (in Gb's)
   * @param cores Number of cores requested per job submission
   * @param maxRunTime Time limit (in hours) after which a job may get killed
   * @param expr Block of cmd's and native code
   * @param scriptContext Container for compile time and run time context for a script
   */
  @deprecated("ugerWith { ... } blocks are deprecated; use drmWith { ... } instead.", "")
  def ugerWith[A](
      cores: Int = -1,
      mem: Double = -1, 
      maxRunTime: Double = -1)
      (expr: => A)
      (implicit scriptContext: LoamScriptContext): A = {
    
    drmWith(cores, mem, maxRunTime)(expr)(scriptContext)
  }
  
  /**
   * @param mem Memory requested per core, per job submission (in Gb's)
   * @param cores Number of cores requested per job submission
   * @param maxRunTime Time limit (in hours) after which a job may get killed
   * @param expr Block of cmd's and native code
   * @param scriptContext Container for compile time and run time context for a script
   */
  def drmWith[A](
      cores: Int = -1,
      mem: Double = -1, 
      maxRunTime: Double = -1,
      imageName: String = "")
      (expr: => A)
      (implicit scriptContext: LoamScriptContext): A = {
    
    def orDefault[B](actual: B, default: B) = if(actual == -1) default else actual
    
    val loamConfig = scriptContext.config
    
    require(
        loamConfig.drmSystem.isDefined, 
        "A DRM system (Uger or LSF) must be specified on the command-line to run jobs in a DRM environment")
    
    val drmSystem = loamConfig.drmSystem.get
    
    val drmConfig: DrmConfig = drmSystem.config(scriptContext)
    
    val useDocker = imageName.nonEmpty
    
    val dockerParamsOpt: Option[DockerParams] = {
      if(useDocker) { Some(DockerParams(imageName)) } 
      else { None }
    }
    
    val settings = drmSystem.settingsMaker(
        Cpus(orDefault(cores, drmConfig.defaultCores.value)), 
        Memory.inGb(orDefault(mem, drmConfig.defaultMemoryPerCore.gb)), 
        CpuTime.inHours(orDefault(maxRunTime, drmConfig.defaultMaxRunTime.hours)),
        drmSystem.defaultQueue,
        dockerParamsOpt)
    
    runIn(drmSystem.makeEnvironment(settings))(expr)(scriptContext)
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

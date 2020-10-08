package loamstream.cli

import java.net.URI
import java.nio.file.Path
import java.nio.file.Paths

import org.rogach.scallop.listArgConverter
import org.rogach.scallop.ScallopConf
import org.rogach.scallop.ScallopOption
import org.rogach.scallop.ValueConverter
import org.rogach.scallop.exceptions.ScallopException

import loamstream.drm.DrmSystem
import loamstream.util.IoUtils
import loamstream.util.Loggable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Buffer

/**
 * Provides a command line interface for LoamStream apps
 * using [[https://github.com/scallop/scallop Scallop]] under the hood
 *
 * @param arguments command line arguments provided by the app user
 * @param exitTheJvmOnValidationError whether or not to exit the whole JVM on any validation errors; setting this
 * to false is useful for tests, so that a validation failure doesn't make SBT exit.  If this is false, a 
 * CliException is thrown instead of invoking 'sys.exit()'.
 */
final case class Conf private (arguments: Seq[String]) extends ScallopConf(arguments) with Loggable {
  
  def printHelp(message: String): Unit = {
    printHelp()
    
    IoUtils.printToConsole()
    
    IoUtils.printToConsole(message)
  }
  
  /** Inform the user about expected usage upon erroneous input/behaviour. */
  override def onError(e: Throwable): Unit = e match {
    case e @ ScallopException(message) => {
      printHelp(message)
      
      throw new CliException(e)
    }
    case ex => super.onError(ex)
  }

  private def printHelpIfNoArgsAndExit(): Unit = {
    if (arguments.isEmpty) {
      printHelp()
    }
  }

  private val listPathConverter: ValueConverter[Seq[Path]] = listArgConverter[Path](Paths.get(_)).map(_.toSeq)
  
  // TODO: Add version info (ideally from build.sbt)?
  banner("""LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
           |Usage: scala loamstream.jar [options] [loam file(s)]
           |       or if you have an executable produced by SBT Native Packager:
           |       loamstream [options] [loam file(s)]
           |Options:
           |""".stripMargin)
  
           
  /** Whether to show version info and quit */         
  val version: ScallopOption[Boolean] = opt[Boolean](descr = "Print version information and exit")
  
  /** Whether to show usage info and quit */
  val help: ScallopOption[Boolean] = opt[Boolean](descr = "Show help and exit")
  
  /** Whether we're running in worker mode (running on a DRM system on behalf of another LS instance) */
  val worker: ScallopOption[Boolean] = opt[Boolean](
      descr = "Run in worker mode (run on a DRM system on behalf of another LS instance)")
  
  val workDir: ScallopOption[Path] = opt[Path](
      descr = "Path to store logs, db files, and job metadata in, analogous to the default .loamstream.  Only " +
              "respected if --worker is also supplied.")
  
  /** Whether to clear the DB */
  val cleanDb: ScallopOption[Boolean] = opt[Boolean](descr = "Clean db")
  
  /** Whether to clear the logs */
  val cleanLogs: ScallopOption[Boolean] = opt[Boolean](descr = "Clean logs")
  
  /** Whether to clear DRM scripts */
  val cleanScripts: ScallopOption[Boolean] = opt[Boolean](descr = "Clean generated DRM (Uger, LSF) scripts")
  
  val clean: ScallopOption[Boolean] = opt[Boolean](
      descr = "Deletes .loamstream/ ; Effectively the same as using all of " +
              "--clean-db, --clean-logs, and --clean-scripts")
  
  val run: ScallopOption[List[String]] = opt(
      descr = "How to run jobs: " +
              "<everything> - always run jobs, never skip them; " +
              "<allOf> <regexes> - run jobs if their names match ALL of the passed regexes" +
              "<anyOf> <regexes> - run jobs if their names match ANY of the passed regexes" +
              "<noneOf> <regexes> - run jobs if their names match NONE of the passed regexes",
      required = false)
  
  val noValidation: ScallopOption[Boolean] = opt[Boolean](
      descr = "Don't validate the graph produced by evaluating .loam files")
      
  /** 
   *  Compile loam files (evaluate them to produce a LoamGraph, or print compilation errors), 
   *  but not execute the jobs declared in them 
   */
  val compileOnly: ScallopOption[Boolean] = opt[Boolean](
      descr = "Only compile the supplied .loam files, don't run them")
      
  /** 
   *  Print a list of what jobs would be executed, without executing anything
   */
  val dryRun: ScallopOption[Boolean] = opt[Boolean](
      descr = "Show what commands would be run without running them")

  /**
   *  Path to LoamStream's config file
   */
  val conf: ScallopOption[Path] = opt[Path](descr = "Path to config file")

  /**
   *  The .loam files to evaluate
   */
  val loams: ScallopOption[Seq[Path]] = opt[Seq[Path]](
      descr = "Path(s) to loam script(s)",
      required = false,
      validate = _.nonEmpty)(listPathConverter)

  /**
   * Don't hash job outputs when determining whether jobs may be skipped
   */
  val disableHashing: ScallopOption[Boolean] = opt[Boolean](
      descr = "Don't hash files when determining whether a job may be skipped.",
      required = false)
  
  /**
   * The DRM backend to use, wither 'lsf' or 'uger'
   */
  val backend: ScallopOption[String] = {
    opt(descr = s"The backend to run jobs on. Options are: ${DrmSystem.values.map(_.name).mkString(",")}")
  }
  
  //NB: Makes Scallop behave
  val trailing: ScallopOption[List[String]] = trailArg(required = false)
  
  //NB: Required by Scallop
  verify()
  
  def toValues: Conf.Values = {
    def getRun: Option[(String, Seq[String])] = run.toOption.flatMap {
      case head +: rest => Some(head -> rest)
      case _            => None
    }
      
    Conf.Values(
      loams = loams.toOption.toSeq.flatten,
      cleanDbSupplied = cleanDb.isSupplied || clean.isSupplied,
      cleanLogsSupplied = cleanLogs.isSupplied || clean.isSupplied,
      cleanScriptsSupplied = cleanScripts.isSupplied || clean.isSupplied,
      conf = conf.toOption,
      helpSupplied = help.isSupplied,
      versionSupplied = version.isSupplied,
      noValidationSupplied = noValidation.isSupplied,
      compileOnlySupplied = compileOnly.isSupplied,
      dryRunSupplied = dryRun.isSupplied,
      disableHashingSupplied = disableHashing.isSupplied,
      backend = backend.toOption,
      run = getRun,
      workerSupplied = worker.isSupplied,
      workDir = workDir.toOption,
      this)
  }
}

object Conf {
  
  def apply(args: Iterable[String]): Conf = new Conf(args.toVector)
  
  object RunStrategies {
    val Everything = "everything"
    val AllOf = "allOf"
    val AnyOf = "anyOf"
    val NoneOf = "noneOf"
  }
  
  final case class Values(
      loams: Seq[Path],
      cleanDbSupplied: Boolean,
      cleanLogsSupplied: Boolean,
      cleanScriptsSupplied: Boolean,
      conf: Option[Path],
      helpSupplied: Boolean,
      versionSupplied: Boolean,
      noValidationSupplied: Boolean,
      compileOnlySupplied: Boolean,
      dryRunSupplied: Boolean,
      disableHashingSupplied: Boolean,
      backend: Option[String],
      run: Option[(String, Seq[String])],
      workerSupplied: Boolean,
      workDir: Option[Path],
      derivedFrom: Conf) {
    
    def withIsWorker(isWorker: Boolean): Values = copy(workerSupplied = true)
    
    def onlyRun(jobName: String): Values = copy(run = Some((RunStrategies.AllOf, Seq(jobName))))
    
    def withBackend(drmSystem: DrmSystem): Values = copy(backend = Some(drmSystem.name.toLowerCase))
    
    def withWorkDir(workDir: Path): Values = copy(workDir = Option(workDir)) 
    
    def confSupplied: Boolean = conf.isDefined
    
    /*
     * Rebuild a set of arguments suitable for running LS from the command line, based on this instance's values.
     *  
     * This method is complex and error-prone - if a field is added to this class, it will need to be handled here
     * as well - but it beats the alternative. :\ If we want to invoke LS with modified settings based on the ones 
     * used for the current run, it's much nicer to flip a flag or two here (see the with* methods) and then build
     * a new set of arguments than to grovel through a Seq[String], munging, adding, or removing values. :\  
     */
    def toArguments: Seq[String] = {
      val conf = derivedFrom
        
      val cleanParts: Seq[String] = {
        val cleanEverything = cleanDbSupplied && cleanLogsSupplied && cleanScriptsSupplied
        
        def cleanSomething = cleanDbSupplied || cleanLogsSupplied || cleanScriptsSupplied
        
        if(cleanEverything) { 
          Seq(asArgName(conf.clean)) 
        } else if (cleanSomething) {
          Seq(
            asStringIfSupplied(cleanDbSupplied, conf.cleanDb),
            asStringIfSupplied(cleanLogsSupplied, conf.cleanLogs),
            asStringIfSupplied(cleanScriptsSupplied, conf.cleanScripts)).filter(_.nonEmpty)
        } else {
          Nil
        }
      }
      
      val confPart: Seq[String] = {
        conf.conf.toOption.toSeq.flatMap(confFilePath => Seq(asArgName(conf.conf), confFilePath.toString))
      }
      
      val noValidationPart: Seq[String] = Seq(asStringIfSupplied(noValidationSupplied, conf.noValidation))
      val compileOnlyPart: Seq[String] = Seq(asStringIfSupplied(compileOnlySupplied, conf.compileOnly))
      val dryRunPart: Seq[String] = Seq(asStringIfSupplied(dryRunSupplied, conf.dryRun))
      val disableHashingPart: Seq[String] = Seq(asStringIfSupplied(disableHashingSupplied, conf.disableHashing)) 
      
      val backendPart: Seq[String] = {
        conf.backend.toOption.toSeq.flatMap(backend => Seq(asArgName(conf.backend), backend.toLowerCase))
      } 
      
      val runPart: Seq[String] = run.toSeq.flatMap { case (what, hows) => asArgName(conf.run) +: what +: hows }
      
      val workerPart: Seq[String] = Seq(asStringIfSupplied(workerSupplied, conf.worker))
      
      val loamsPart: Seq[String] = {
        if(loams.nonEmpty) { asArgName(conf.loams) +: loams.map(_.toString) }
        else { Nil }
      }
      
      val result: Buffer[String] = new ListBuffer
      
      result ++= workerPart ++= cleanParts ++= confPart ++= noValidationPart ++= compileOnlyPart ++= dryRunPart ++= 
      disableHashingPart ++= backendPart ++= runPart ++= loamsPart
      
      result.toList.filter(_.nonEmpty)
    }
  }
  
  private def asStringIfSupplied(flag: Boolean, sOpt: ScallopOption[_]): String = {
    if(flag) asArgName(sOpt) else ""
  }
  
  private def asArgName(arg: ScallopOption[_]): String = {
    val argName = arg.name
    
    val hyphens = argName.size match {
      case 1 => "-"
      case _ => "--"
    }
    
    s"${hyphens}${argName}"
  }
}

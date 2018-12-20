package loamstream.cli

import java.net.URI
import java.nio.file.Path

import org.rogach.scallop.ScallopOption

import loamstream.drm.DrmSystem
import loamstream.model.execute.HashingStrategy
import loamstream.util.Loggable


/**
 * @author clint
 * Dec 13, 2017
 */
sealed trait Intent {
  def confFile: Option[Path]
}

object Intent extends Loggable {

  final case object ShowVersionAndQuit extends Intent {
    override def confFile: Option[Path] = None
  }
  
  final case object ShowHelpAndQuit extends Intent {
    override def confFile: Option[Path] = None
  }

  final case class LookupOutput(confFile: Option[Path], output: Either[Path, URI]) extends Intent

  final case class Clean(confFile: Option[Path], db: Boolean, logs: Boolean, scripts: Boolean) extends Intent
  
  final case class CompileOnly(
      confFile: Option[Path], 
      shouldValidate: Boolean,
      drmSystemOpt: Option[DrmSystem], 
      loams: Seq[Path]) extends Intent

  final case class DryRun(
      confFile: Option[Path],
      shouldValidate: Boolean,
      hashingStrategy: HashingStrategy,
      jobFilterIntent: JobFilterIntent,
      drmSystemOpt: Option[DrmSystem],
      loams: Seq[Path]) extends Intent {
    
    def shouldRunEverything: Boolean = jobFilterIntent == JobFilterIntent.RunEverything 
  }

  final case class RealRun(
      confFile: Option[Path],
      shouldValidate: Boolean,
      hashingStrategy: HashingStrategy,
      jobFilterIntent: JobFilterIntent,
      drmSystemOpt: Option[DrmSystem],
      loams: Seq[Path]) extends Intent {
    
    def shouldRunEverything: Boolean = jobFilterIntent == JobFilterIntent.RunEverything
  }

  def from(cli: Conf): Either[String, Intent] = from(cli.toValues)
    
  private def from(values: Conf.Values): Either[String, Intent] = {
    import java.nio.file.Files.exists

    def noLoamsSupplied = values.loams.isEmpty

    def loamsThatDontExist: Seq[Path] = values.loams.filter(!exists(_))

    val result = {
      asShowVersionAndQuit(values) orElse
      asShowHelpAndQuit(values) orElse
      asClean(values) orElse
      asLookupOutput(values) orElse 
      asCompileOnly(values) orElse
      asDryRun(values) orElse 
      asRealRun(values)
    }

    result match {
      case Some(intent) => Right(intent)
      case None => {
        if (confSpecifiedButDoesntExist(values)) { 
          Left(s"Config file '${values.conf.get}' specified, but it doesn't exist.") 
        } else if (noLoamsSupplied) { 
          Left("No loam files specified.") 
        } else if (!allLoamsExist(values)) { 
          Left(s"Some loam files were missing: ${loamsThatDontExist.mkString(", ")}") 
        } else { 
          Left("Malformed command line.") 
        }
      }
    }
  }
  
  import java.nio.file.Files.exists
  
  private def confExistsOrOmitted(values: Conf.Values): Boolean = values.conf match {
    case Some(confFile) => exists(confFile)
    case None => true
  }
    
  private def confSpecifiedButDoesntExist(values: Conf.Values) = !confExistsOrOmitted(values)
  
  private def asShowVersionAndQuit(values: Conf.Values): Option[Intent] = {
    if(values.versionSupplied) Some(ShowVersionAndQuit) else None
  }
    
  private def asShowHelpAndQuit(values: Conf.Values): Option[Intent] = {
    if(values.helpSupplied) Some(ShowHelpAndQuit) else None
  }
  
  private def asClean(values: Conf.Values): Option[Intent] = {
    if(values.cleanDbSupplied || values.cleanLogsSupplied || values.cleanScriptsSupplied) {
      Some(Clean(
          values.conf, 
          db = values.cleanDbSupplied, 
          logs = values.cleanLogsSupplied,
          scripts = values.cleanScriptsSupplied))
    } else {
      None
    }
  }
  
  private def asLookupOutput(values: Conf.Values): Option[Intent] = {
    if(confExistsOrOmitted(values) && values.lookupSupplied) {
      Some(LookupOutput(values.conf, values.lookup.get)) 
    } else {
      None
    }
  }
  
  private def asCompileOnly(values: Conf.Values): Option[Intent] = {
    if(confExistsOrOmitted(values) && compileOnly(values)) {
      Some(CompileOnly(values.conf, !values.noValidationSupplied, getDrmSystem(values), values.loams))
    } else {
      None
    }
  }
  
  private def asDryRun(values: Conf.Values): Option[Intent] = {
    if(confExistsOrOmitted(values) && dryRun(values) && allLoamsExist(values)) Some(makeDryRun(values)) else None  
  }
  
  private def asRealRun(values: Conf.Values): Option[Intent] = {
    if(confExistsOrOmitted(values) && allLoamsExist(values)) { Some(makeRealRun(values)) } else None
  }
  
  private def makeDryRun(values: Conf.Values): DryRun = {
    DryRun(
      confFile = values.conf,
      shouldValidate = !values.noValidationSupplied, 
      hashingStrategy = determineHashingStrategy(values),
      jobFilterIntent = determineJobFilterIntent(values),
      drmSystemOpt = getDrmSystem(values),
      loams = values.loams)
  }
  
  private def makeRealRun(values: Conf.Values): RealRun = {
    RealRun(
      confFile = values.conf,
      shouldValidate = !values.noValidationSupplied,
      hashingStrategy = determineHashingStrategy(values),
      jobFilterIntent = determineJobFilterIntent(values),
      drmSystemOpt = getDrmSystem(values),
      loams = values.loams)
  }

  private def getDrmSystem(values: Conf.Values): Option[DrmSystem] = for {
    backend <- values.backend
    drmSystem <- DrmSystem.fromName(backend)
  } yield drmSystem
  
  private def dryRun(values: Conf.Values) = values.dryRunSupplied && allLoamsExist(values)

  private def compileOnly(values: Conf.Values) = values.compileOnlySupplied && allLoamsExist(values)

  private def allLoamsExist(values: Conf.Values) = {
    values.loams.nonEmpty && values.loams.forall(exists(_))
  }

  private[cli] def determineHashingStrategy(values: Conf.Values): HashingStrategy = {
    import loamstream.model.execute.HashingStrategy.{ DontHashOutputs, HashOutputs }

    if (values.disableHashingSupplied) DontHashOutputs else HashOutputs
  }
  
  private[cli] def determineJobFilterIntent(values: Conf.Values): JobFilterIntent = {
    def nameOf(field: Conf => ScallopOption[_]) = field(values.derivedFrom).name
    
    def toRegexes(regexStrings: Seq[String]) = regexStrings.map(_.r)
    
    import JobFilterIntent._
    
    values.run match {
      case Some((Conf.RunStrategies.Everything, _)) => RunEverything
      case Some((Conf.RunStrategies.AllOf, regexStrings)) => RunIfAllMatch(toRegexes(regexStrings))
      case Some((Conf.RunStrategies.AnyOf, regexStrings)) => RunIfAnyMatch(toRegexes(regexStrings))
      case Some((Conf.RunStrategies.NoneOf, regexStrings)) => RunIfNoneMatch(toRegexes(regexStrings))
      case _ => DontFilterByName
    }
  }
}

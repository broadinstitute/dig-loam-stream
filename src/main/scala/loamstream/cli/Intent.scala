package loamstream.cli

import java.nio.file.Path
import loamstream.model.execute.HashingStrategy
import java.net.URI
import loamstream.util.Loggable
import loamstream.drm.DrmSystem
import org.rogach.scallop.ScallopOption

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

  final case class CompileOnly(confFile: Option[Path], loams: Seq[Path]) extends Intent

  final case class DryRun(
      confFile: Option[Path],
      hashingStrategy: HashingStrategy,
      jobFilterIntent: JobFilterIntent,
      loams: Seq[Path]) extends Intent {
    
    def shouldRunEverything: Boolean = jobFilterIntent == JobFilterIntent.RunEverything 
  }

  final case class RealRun(
      confFile: Option[Path],
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

    def confDoesntExist = values.conf match {
      case Some(confFile) => !exists(confFile)
      case None => false
    }

    def allLoamsExist = values.loams.nonEmpty && values.loams.forall(exists(_))

    if (values.versionSupplied) { Right(ShowVersionAndQuit) }
    else if (values.helpSupplied) { Right(ShowHelpAndQuit) }
    else if (confDoesntExist) { Left(s"Config file '${values.conf.get}' specified, but it doesn't exist.") }
    else if (values.lookupSupplied) { Right(LookupOutput(values.conf, values.lookup.get)) }
    else if (compileOnly(values)) { Right(CompileOnly(values.conf, values.loams)) }
    else if (dryRun(values) && allLoamsExist) { Right(makeDryRun(values)) } 
    else if (allLoamsExist) { Right(makeRealRun(values)) } 
    else if (noLoamsSupplied) { Left("No loam files specified.") } 
    else if (!allLoamsExist) { Left(s"Some loam files were missing: ${loamsThatDontExist.mkString(", ")}") } 
    else { Left("Malformed command line.") }
  }

  private def makeDryRun(values: Conf.Values): DryRun = {
    DryRun(
      confFile = values.conf,
      hashingStrategy = determineHashingStrategy(values),
      jobFilterIntent = determineJobFilterIntent(values),
      loams = values.loams)
  }

  private def makeRealRun(values: Conf.Values): RealRun = {
   
    val drmSystemOpt: Option[DrmSystem] = for {
      backend <- values.backend
      drmSystem <- DrmSystem.fromName(backend)
    } yield drmSystem

    RealRun(
      confFile = values.conf,
      hashingStrategy = determineHashingStrategy(values),
      jobFilterIntent = determineJobFilterIntent(values),
      drmSystemOpt = drmSystemOpt,
      loams = values.loams)
  }

  private def dryRun(values: Conf.Values) = values.dryRunSupplied && allLoamsExist(values)

  private def compileOnly(values: Conf.Values) = values.compileOnlySupplied && allLoamsExist(values)

  private def allLoamsExist(values: Conf.Values) = {
    import java.nio.file.Files.exists

    values.loams.nonEmpty && values.loams.forall(exists(_))
  }

  private[cli] def determineHashingStrategy(values: Conf.Values): HashingStrategy = {
    import HashingStrategy.{ DontHashOutputs, HashOutputs }

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

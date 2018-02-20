package loamstream.cli

import java.nio.file.Path
import loamstream.model.execute.HashingStrategy
import java.net.URI
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
    def confFile: Option[Path] = None
  }

  final case class LookupOutput(confFile: Option[Path], output: Either[Path, URI]) extends Intent

  final case class CompileOnly(confFile: Option[Path], loams: Seq[Path]) extends Intent

  final case class DryRun(
    confFile: Option[Path],
    hashingStrategy: HashingStrategy,
    shouldRunEverything: Boolean,
    loams: Seq[Path]) extends Intent

  final case class RealRun(
    confFile: Option[Path],
    hashingStrategy: HashingStrategy,
    shouldRunEverything: Boolean,
    loams: Seq[Path]) extends Intent

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
    else if (confDoesntExist) { Left(s"Config file '${values.conf.get}' specified, but it doesn't exist.") }
    else if (values.lookupSupplied) { Right(LookupOutput(values.conf, values.lookup.get)) }
    else if (compileOnly(values)) { Right(CompileOnly(values.conf, values.loams)) }
    else if (dryRun(values)) {
      Right(makeDryRun(values))
    } else if (allLoamsExist) {
      Right(makeRealRun(values))
    } else if (noLoamsSupplied) {
      Left("No loam files specified.")
    } else if (!allLoamsExist) {
      Left(s"Some loam files were missing: ${loamsThatDontExist.mkString(", ")}")
    } else {
      Left("Malformed command line.")
    }
  }

  private def makeDryRun(values: Conf.Values): DryRun = DryRun(
    confFile = values.conf,
    hashingStrategy = determineHashingStrategy(values),
    shouldRunEverything = values.runEverythingSupplied,
    loams = values.loams)

  private def makeRealRun(values: Conf.Values): RealRun = RealRun(
    confFile = values.conf,
    hashingStrategy = determineHashingStrategy(values),
    shouldRunEverything = values.runEverythingSupplied,
    loams = values.loams)

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
}

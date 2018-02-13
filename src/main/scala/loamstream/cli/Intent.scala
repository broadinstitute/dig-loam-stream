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
  
  def from(cli: Conf): Either[String, Intent] = {
    import java.nio.file.Files.exists
    
    def confOpt = cli.conf.toOption
    
    def noLoams = !cli.loams.isSupplied || cli.loams().isEmpty
    
    def anyLoamsPresent = cli.loams.isSupplied && cli.loams().nonEmpty
    
    def loamsThatDontExist: Seq[Path] = if(anyLoamsPresent) cli.loams().filter(!exists(_)) else Nil
    
    def allLoamsPresent = anyLoamsPresent && cli.loams().forall(exists(_))
    
    def confDoesntExist = cli.conf.isSupplied && !exists(cli.conf())
    
    def loams = cli.loams()
    
    def shouldRunEverything = cli.runEverything.isSupplied
    
    def hashingStrategy = determineHashingStrategy(cli)
    
    if(cli.version.isSupplied) { Right(ShowVersionAndQuit) }
    else if(confDoesntExist) { Left(s"Config file '${cli.conf()}' specified, but it doesn't exist.") }
    else if(cli.lookup.isSupplied) { Right(LookupOutput(confOpt, cli.lookup())) }
    else if(cli.compileOnly.isSupplied && allLoamsPresent) { Right(CompileOnly(confOpt, loams)) }
    else if(cli.dryRun.isSupplied && allLoamsPresent) { 
      Right(DryRun(
        confFile = confOpt,
        hashingStrategy = hashingStrategy,
        shouldRunEverything = shouldRunEverything,
        loams = loams))
    } else if(allLoamsPresent) {
      Right(RealRun(
        confFile = confOpt,
        hashingStrategy = hashingStrategy,
        shouldRunEverything = shouldRunEverything,
        loams = loams))
    } else if(noLoams) {
      Left("No loam files specified.")
    } else if(!allLoamsPresent) {
      Left(s"Some loam files were missing: ${loamsThatDontExist.mkString(", ")}")
    } else {
      Left("Malformed command line.")
    }
  }
  
  private[cli] def determineHashingStrategy(cli: Conf): HashingStrategy = {
    import HashingStrategy.{DontHashOutputs, HashOutputs}
    
    cli.disableHashing.toOption.map { shouldDisableHashing =>
      if(shouldDisableHashing) DontHashOutputs else HashOutputs
    }.getOrElse {
      HashOutputs
    }
  }
}

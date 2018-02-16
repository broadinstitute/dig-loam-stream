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
    
    def noLoamsSupplied = !anyLoamsSupplied(cli)
    
    def loamsThatDontExist: Seq[Path] = if(anyLoamsSupplied(cli)) cli.loams().filter(!exists(_)) else Nil
    
    def confDoesntExist = cli.conf.isSupplied && !exists(cli.conf())
    
    def loams = cli.loams()
    
    def shouldRunEverything = cli.runEverything.isSupplied
    
    def hashingStrategy = determineHashingStrategy(cli)
    
    def makeDryRun = DryRun(
        confFile = confOpt,
        hashingStrategy = hashingStrategy,
        shouldRunEverything = shouldRunEverything,
        loams = loams)
    
    def makeRealRun = RealRun(
        confFile = confOpt,
        hashingStrategy = hashingStrategy,
        shouldRunEverything = shouldRunEverything,
        loams = loams)
        
    if(cli.version.isSupplied) { Right(ShowVersionAndQuit) }
    else if(confDoesntExist) { Left(s"Config file '${cli.conf()}' specified, but it doesn't exist.") }
    else if(cli.lookup.isSupplied) { Right(LookupOutput(confOpt, cli.lookup())) }
    else if(compileOnly(cli)) { Right(CompileOnly(confOpt, loams)) }
    else if(dryRun(cli)) { 
      Right(makeDryRun)
    } else if(allLoamsExist(cli)) {
      Right(makeRealRun)
    } else if(noLoamsSupplied) {
      Left("No loam files specified.")
    } else if(!allLoamsExist(cli)) {
      Left(s"Some loam files were missing: ${loamsThatDontExist.mkString(", ")}")
    } else {
      Left("Malformed command line.")
    }
  }
  
  private def dryRun(cli: Conf) = cli.dryRun.isSupplied && allLoamsExist(cli)
  
  private def compileOnly(cli: Conf) = cli.compileOnly.isSupplied && allLoamsExist(cli)
  
  private def anyLoamsSupplied(cli: Conf) = cli.loams.isSupplied && cli.loams().nonEmpty
  
  private def allLoamsExist(cli: Conf) = {
    import java.nio.file.Files.exists
    
    anyLoamsSupplied(cli) && cli.loams().forall(exists(_))
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

package loamstream.cli

import java.nio.file.Path
import loamstream.model.execute.HashingStrategy
import java.net.URI

/**
 * @author clint
 * Dec 13, 2017
 */
sealed trait Intent {
  def confFile: Option[Path]
}

object Intent {
  final case object ShowVersionAndQuit extends Intent {
    def confFile: Option[Path] = None
  }
  
  final case class LookupOutput(confFile: Option[Path], output: Either[Path, URI]) extends Intent
  
  final case class CompileOnly(confFile: Option[Path], loams: Seq[Path]) extends Intent
  
  final case class RealRun(
      confFile: Option[Path],
      hashingStrategy: HashingStrategy,
      shouldRunEverything: Boolean,
      loams: Seq[Path]) extends Intent
 
  
  def from(cli: Conf): Intent = {
    def confOpt = cli.conf.toOption
    
    if(cli.version.isSupplied) { ShowVersionAndQuit }
    else if(cli.lookup.isSupplied) { LookupOutput(confOpt, cli.lookup()) }
    else if(cli.compileOnly.isSupplied) { CompileOnly(confOpt, cli.loams()) }
    else {
      RealRun(
        confFile = confOpt,
        hashingStrategy = determineHashingStrategy(cli),
        shouldRunEverything = cli.runEverything.isSupplied,
        loams = cli.loams())
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

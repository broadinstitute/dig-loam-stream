package loamstream.model.execute

import java.net.URI
import java.nio.file.Path

import loamstream.model.jobs.LJob
import loamstream.util.Loggable
import scala.collection.JavaConverters._
import loamstream.util.CanBeClosed
import java.io.StringReader
import java.io.BufferedReader
import java.io.FileReader
import java.io.Reader
import java.nio.file.Paths
import loamstream.model.jobs.DataHandle

/**
 * @author clint
 * Dec 15, 2020
 */
final class ProtectsFilesJobFilter private (
    //NB: Use a java.util.Set for somewhat improved performance, since we expect this class's
    //shouldRun() method to be called _a lot_, on the order of once per store in a pipeline.
    private val _locationsToProtect: java.util.Set[String]) extends JobFilter with Loggable {
  
  private[execute] def locationsToProtect: Set[String] = _locationsToProtect.asScala.toSet
  
  override def shouldRun(job: LJob): Boolean = {
    def isProtected(output: DataHandle): Boolean = _locationsToProtect.contains(output.location)
    
    val anyOutputIsProtected = job.outputs.exists(isProtected)
    
    val noOutputsAreProtected = !anyOutputIsProtected
    
    def protectedOutputs = job.outputs.filter(isProtected).map(_.location).map(l => s"'${l}'")
    
    if(anyOutputIsProtected) {
      info(s"Skipping job $job because the following outputs are protected: ${protectedOutputs.mkString(",")}")
    }
    
    noOutputsAreProtected
  }
}

object ProtectsFilesJobFilter {
  private def asJavaSet[A](as: Iterable[A]): java.util.HashSet[A] = {
    val set = new java.util.HashSet[A]
    
    as.foreach(set.add)
    
    set
  }
  
  def apply(locs: Iterable[String]): ProtectsFilesJobFilter = new ProtectsFilesJobFilter(asJavaSet(locs))
  
  def apply(loc: String, rest: String*): ProtectsFilesJobFilter = apply(loc +: rest)
  
  def apply(pathsOrUris: Iterable[Either[Path, URI]])(implicit discriminator: Int = 42): ProtectsFilesJobFilter = {
    val locs: Iterable[String] = pathsOrUris.collect {
      case Left(p) => p.toAbsolutePath.toString
      case Right(u) => u.toString
    }
    
    apply(locs)
  }
  
  def fromString(data: String): ProtectsFilesJobFilter = fromReader(new StringReader(data))
  
  def fromFile(file: Path): ProtectsFilesJobFilter = fromReader(new FileReader(file.toFile))
  
  def fromReader(reader: Reader): ProtectsFilesJobFilter = {
    CanBeClosed.using(new BufferedReader(reader)) { br =>
      fromLines(br.lines.iterator.asScala)
    }
  }
  
  private def fromLines(lines: Iterator[String]): ProtectsFilesJobFilter = {
    def shouldSkip(line: String): Boolean = {
      line.isEmpty ||
      line.startsWith("#") ||
      line.startsWith("//")
    }
    
    def parse(s: String): Either[Path, URI] = if(s.startsWith("gs://")) Right(URI.create(s)) else Left(Paths.get(s))
    
    val locs = lines.map(_.trim).filterNot(shouldSkip).map(parse)
    
    ProtectsFilesJobFilter(locs.toList)
  }
}

package loamstream.model.execute

import java.io.BufferedReader
import java.io.FileReader
import java.io.Reader
import java.io.StringReader
import java.net.URI
import java.nio.file.Path

import scala.collection.JavaConverters._

import loamstream.model.jobs.DataHandle
import loamstream.model.jobs.LJob
import loamstream.util.CanBeClosed
import loamstream.util.Loggable
import loamstream.util.{Paths => LPaths}
import java.nio.file.Paths
import scala.collection.compat._

/**
 * @author clint
 * Dec 15, 2020
 */
final class ProtectsFilesJobCanceler private (
    //NB: Use a java.util.Set for somewhat improved performance, since we expect this class's
    //shouldRun() method to be called _a lot_, on the order of once per store in a pipeline.
    private val _locationsToProtect: java.util.Set[String]) extends JobCanceler with Loggable {
  
  doInitialLogging()
  
  private def doInitialLogging(): Unit = {
    import _locationsToProtect.size
    
    debug(s"Made ${ProtectsFilesJobCanceler.getClass.getSimpleName} that will protect ${size} locations:")
    
    if(isDebugEnabled) {
      locationsToProtect.to(Seq).sorted.foreach(debug(_))
    }
  }
  
  val isEmpty: Boolean = _locationsToProtect.isEmpty
  
  def nonEmpty: Boolean = !isEmpty
  
  override def hashCode: Int = _locationsToProtect.hashCode
  
  override def equals(other: Any): Boolean = other match {
    case that: ProtectsFilesJobCanceler => this.locationsToProtect == that.locationsToProtect
    case _ => false
  }
  
  private[execute] def locationsToProtect: Set[String] = _locationsToProtect.asScala.toSet
  
  override def shouldCancel(job: LJob): Boolean = {
    def isProtected(output: DataHandle): Boolean = _locationsToProtect.contains(output.location)
    
    val outputs = job.outputs
    
    def anyOutputIsProtected = outputs.exists(isProtected)

    //Short-circuit if there are no protected outputs to test against; we expect this method 
    //will be called a lot - once per job in every pipeline.
    val jobShouldBeCanceled = nonEmpty && anyOutputIsProtected
    
    if(jobShouldBeCanceled) {
      def toQuotedString(handle: DataHandle): String = s"'${handle.location}'"
      
      def protectedOutputs = outputs.filter(isProtected).map(toQuotedString)
      
      info(s"Cancelling job $job because the following outputs are protected: ${protectedOutputs.mkString(",")}")
    }
    
    jobShouldBeCanceled
  }
}

object ProtectsFilesJobCanceler {
  private def asJavaSet[A](as: Iterable[A]): java.util.HashSet[A] = {
    val set = new java.util.HashSet[A]
    
    as.foreach(set.add)
    
    set
  }
  
  def apply(locs: Iterable[String]): ProtectsFilesJobCanceler = new ProtectsFilesJobCanceler(asJavaSet(locs))
  
  def apply(loc: String, rest: String*): ProtectsFilesJobCanceler = apply(loc +: rest)
  
  private[execute] def fromEithers(
      pathsOrUris: Iterable[Either[Path, URI]])(implicit discriminator: Int = 1): ProtectsFilesJobCanceler = {
    
    val locs: Iterable[String] = pathsOrUris.collect {
      case Left(p) => LPaths.normalize(p).toString
      case Right(u) => u.toString
    }
    
    new ProtectsFilesJobCanceler(asJavaSet(locs))
  }

  def empty: ProtectsFilesJobCanceler = apply(Seq.empty[String])
  
  def fromString(data: String): ProtectsFilesJobCanceler = fromReader(new StringReader(data))
  
  def fromFile(file: Path): ProtectsFilesJobCanceler = fromReader(new FileReader(file.toFile))
  
  def fromReader(reader: Reader): ProtectsFilesJobCanceler = {
    CanBeClosed.using(new BufferedReader(reader)) { br =>
      fromLines(br.lines.iterator.asScala)
    }
  }
  
  private def fromLines(lines: Iterator[String]): ProtectsFilesJobCanceler = {
    def shouldSkip(line: String): Boolean = {
      line.isEmpty ||
      line.startsWith("#") ||
      line.startsWith("//")
    }
    
    def parse(s: String): Either[Path, URI] = if(s.startsWith("gs://")) Right(URI.create(s)) else Left(Paths.get(s))
    
    val locs = lines.map(_.trim).filterNot(shouldSkip).map(parse)
    
    fromEithers(locs.toList)
  }
}

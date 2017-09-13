package loamstream.util

import scala.util.Try
import java.util.Properties
import java.time.Instant
import java.io.Reader
import java.io.InputStreamReader

/**
 * @author clint
 * Oct 28, 2016
 */
final case class Versions(
    name: String, 
    version: String, 
    branch: String, 
    lastCommit: Option[String], 
    anyUncommittedChanges: Boolean,
    describedVersion: Option[String],
    buildDate: Instant) {
  
  override def toString: String = {
    val isDirtyPhrase = if(anyUncommittedChanges) " (PLUS uncommitted changes!) " else " "
    
    val branchPhrase = s"branch: $branch"
      
    val describedVersionPhrase = describedVersion.getOrElse("UNKNOWN")
    
    val commitPhrase = s"commit: ${lastCommit.getOrElse("UNKNOWN")}"
    
    val buildDatePhrase = s"built on: $buildDate"
      
    s"$name $version ($describedVersionPhrase) $branchPhrase $commitPhrase$isDirtyPhrase$buildDatePhrase"
  }
}

object Versions {
  def load(): Try[Versions] = {
    val versionsPropsFile = "versionInfo.properties"
    
    val propStreamOption = Option(getClass.getClassLoader.getResourceAsStream(versionsPropsFile))
    
    val propStreamAttempt = Options.toTry(propStreamOption)(s"Couldn't find '$versionsPropsFile' on the classpath")
    
    for {
      propStream <- propStreamAttempt
      reader <- Try(new InputStreamReader(propStream))
      versions <- loadFrom(reader)
    } yield {
      versions
    }
  }
  
  private[util] def loadFrom(reader: Reader): Try[Versions] = {
    for {
      props <- toProps(reader)
      name <- props.tryGetProperty("name")
      version <- props.tryGetProperty("version")
      branch <- props.tryGetProperty("branch")
      lastCommit = props.tryGetProperty("lastCommit").toOption
      anyUncommittedChanges <- props.tryGetProperty("uncommittedChanges").map(_.toBoolean)
      describedVersion = props.tryGetProperty("describedVersion").toOption
      buildDate <- props.tryGetProperty("buildDate").map(Instant.parse)
    } yield {
      Versions(name, version, branch, lastCommit, anyUncommittedChanges, describedVersion, buildDate)
    }
  }
  
  private final implicit class PropertiesOps(val props: Properties) extends AnyVal {
    def tryGetProperty(key: String): Try[String] = {
      Options.toTry(Option(props.getProperty(key)).map(_.trim).filter(_.nonEmpty)) {
        import scala.collection.JavaConverters._
        
        val sortedPropKvPairs = props.asScala.toSeq.sortBy { case (k, _) => k }
        
        s"property key '$key' not found in $sortedPropKvPairs"
      }
    }
  }
  
  private def toProps(reader: Reader): Try[Properties] = Try {
    CanBeClosed.enclosed(reader) { _ =>
      val props = new Properties
          
      props.load(reader)
          
      props
    }
  }
}
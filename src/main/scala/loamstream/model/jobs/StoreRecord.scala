package loamstream.model.jobs

import java.net.URI
import java.nio.file.Path
import java.time.Instant

import loamstream.util.Paths

/**
 * @author kyuksel
 * Dec 12, 2016
 *
 * A container for job output attributes that are to be recorded and are not system-dependent
 * (e.g. in hash type or how resources are identified [URI/Path/etc])
 */
final case class StoreRecord(loc: String,
                              isPresent: Boolean,
                              hash: Option[String],
                              hashType: Option[String],
                              lastModified: Option[Instant]) {

  def isMissing: Boolean = !isPresent

  def hasDifferentModTimeThan(other: StoreRecord): Boolean = {
    val resultOpt = for {
      timestamp <- lastModified
      otherTimestamp <- other.lastModified
    } yield timestamp != otherTimestamp
    
    resultOpt.getOrElse(false)
  }

  def isHashed: Boolean = hash.isDefined

  def hasDifferentHashThan(other: StoreRecord): Boolean = (
    for {
      hashValue <- hash
      hashKind <- hashType
      otherHashValue <- other.hash
      otherHashKind <- other.hashType
    } yield hashKind != otherHashKind || hashValue != otherHashValue
    ).getOrElse(false)

  def withLastModified(t: Instant) = copy(lastModified = Option(t))

  override def toString: String = loc
  
  def toVerboseString: String = s"${getClass.getSimpleName}($loc, $isPresent, $hash, $hashType, $lastModified)"
}

object StoreRecord {
  def apply(loc: String): StoreRecord = StoreRecord(loc,
                                                      isPresent = false,
                                                      hash = None,
                                                      hashType = None,
                                                      lastModified = None)
                                                      
  def apply(path: Path): StoreRecord = StoreRecord(Paths.normalize(path))
  
  def apply(uri: URI): StoreRecord = StoreRecord(uri.toString)

  def apply(loc: String,
            hash: Option[String],
            hashType: Option[String],
            lastModified: Option[Instant]): StoreRecord = StoreRecord(loc,
                                                                        isPresent = lastModified.isDefined,
                                                                        hash = hash,
                                                                        hashType = hashType,
                                                                        lastModified = lastModified)

  

  def apply(output: DataHandle): StoreRecord = output.lastModified match {
    case lmOpt @ Some(_) => StoreRecord( loc = output.location,
                                          isPresent = true,
                                          hash = output.hash.map(_.valueAsBase64String),
                                          hashType = output.hashType.map(_.algorithmName),
                                          lastModified = lmOpt)
    case _ => StoreRecord( loc = output.location,
                            isPresent = false,
                            hash = None,
                            hashType = None,
                            lastModified = None)
  }
}

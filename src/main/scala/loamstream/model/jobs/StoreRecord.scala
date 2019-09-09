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
final class StoreRecord private (
    val loc: String,
    val isPresent: Boolean,
    private val makeHash: () => Option[String],
    private val makeHashType: () => Option[String],
    val lastModified: Option[Instant]) {

  lazy val hash: Option[String] = makeHash()
  lazy val hashType: Option[String] = makeHashType()
  
  def copy(
      loc: String = this.loc,
      isPresent: Boolean = this.isPresent,
      lastModified: Option[Instant] = this.lastModified): StoreRecord = {
    new StoreRecord(loc, isPresent, makeHash, makeHashType, lastModified)
  }
  
  //NB: This will force the evaluation of hash and hashType!
  override def equals(other: Any): Boolean = other match {
    case that: StoreRecord => {
      this.loc == that.loc &&
      this.isPresent == that.isPresent &&
      this.hash == that.hash &&
      this.hashType == that.hashType &&
      this.lastModified == that.lastModified
    }
    case _ => false
  }
  
  //NB: This will force the evaluation of hash and hashType! 
  override def hashCode: Int = List(loc, isPresent, hash, hashType, lastModified).hashCode
  
  def isMissing: Boolean = !isPresent

  def hasDifferentModTimeThan(other: StoreRecord): Boolean = {
    val resultOpt = for {
      timestamp <- lastModified
      otherTimestamp <- other.lastModified
    } yield timestamp != otherTimestamp
    
    resultOpt.getOrElse(false)
  }

  def isHashed: Boolean = hash.isDefined

  def hasDifferentHashThan(other: StoreRecord): Boolean = {
    (for {
      hashValue <- hash
      hashKind <- hashType
      otherHashValue <- other.hash
      otherHashKind <- other.hashType
    } yield {
      hashKind != otherHashKind || hashValue != otherHashValue
    }).getOrElse(false)
  }

  def withLastModified(t: Instant) = copy(lastModified = Option(t))

  override def toString: String = loc
  
  def toVerboseString: String = s"${getClass.getSimpleName}($loc, $isPresent, $hash, $hashType, $lastModified)"
}

object StoreRecord {
  def apply(loc: String,
            isPresent: Boolean,
            makeHash: () => Option[String],
            makeHashType: () => Option[String],
            lastModified: Option[Instant]): StoreRecord = {
    new StoreRecord(loc, isPresent, makeHash, makeHashType, lastModified) 
  }
  
  def apply(loc: String): StoreRecord = StoreRecord(loc,
                                                    isPresent = false,
                                                    makeHash = () => None,
                                                    makeHashType = () => None,
                                                    lastModified = None)
                                                      
  def apply(path: Path): StoreRecord = StoreRecord(Paths.normalize(path))
  
  def apply(uri: URI): StoreRecord = StoreRecord(uri.toString)

  def apply(loc: String,
            hash: () => Option[String],
            hashType: () => Option[String],
            lastModified: Option[Instant]): StoreRecord = StoreRecord(loc,
                                                                      isPresent = lastModified.isDefined,
                                                                      makeHash = hash,
                                                                      makeHashType = hashType,
                                                                      lastModified = lastModified)

  

  def apply(output: DataHandle): StoreRecord = output.lastModified match {
    case lmOpt @ Some(_) => StoreRecord(loc = output.location,
                                        isPresent = true,
                                        makeHash = () => output.hash.map(_.valueAsBase64String),
                                        makeHashType = () => output.hashType.map(_.algorithmName),
                                        lastModified = lmOpt)
                                        
    case _ => StoreRecord( loc = output.location,
                            isPresent = false,
                            makeHash = () => None,
                            makeHashType = () => None,
                            lastModified = None)
  }
}

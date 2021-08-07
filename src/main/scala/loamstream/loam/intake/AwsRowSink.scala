package loamstream.loam.intake

import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import loamstream.util.Loggable
import loamstream.util.Sequence
import java.util.UUID
import loamstream.util.ValueBox
import scala.util.control.NonFatal
import org.broadinstitute.dig.aws.Implicits
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import loamstream.util.S3Client
import scala.util.Try
import scala.collection.compat._

/**
 * @author clint
 * Dec 4, 2020
 */
object AwsRowSink {
  object Defaults {
    def fileIdSequence: Iterator[Long] = Sequence[Long]().iterator
  
    def randomUUID: String = UUID.randomUUID.toString
  
    val batchSize: Int = 300000
    
    def baseDir: Option[String] = None
  }
  
  def makePath(
    topic: String,
    dataset: String,
    techType: Option[TechType],
    phenotype: Option[String],
    baseDir: Option[String]): String = {
    
    def addTrailingSlashIfNeeded(s: String): String = if(s.endsWith("/")) s else s"${s}/"
    
    val baseDirPart = baseDir.map(addTrailingSlashIfNeeded).getOrElse("")
    
    val techTypePart = techType match {
      case Some(tt) => s"${tt.name}/"
      case None => ""
    }
    
    val phenotypePart = phenotype match {
      case Some(p) => s"/${p}"
      case None => ""
    }
    
    s"${baseDirPart}${topic}/${techTypePart}${dataset}${phenotypePart}"
  }
}

final case class AwsRowSink(
    topic: String,
    dataset: String,
    techType: Option[TechType],
    phenotype: Option[String],
    metadata: JObject,
    s3Client: S3Client,
    batchSize: Int = AwsRowSink.Defaults.batchSize,
    baseDir: Option[String] = AwsRowSink.Defaults.baseDir,
    private val fileIds: Iterator[Long] = AwsRowSink.Defaults.fileIdSequence,
    private val uuid: String = AwsRowSink.Defaults.randomUUID) extends RowSink[RenderableJsonRow] with Loggable {
  
  private[this] val initializedBox: ValueBox[Boolean] = ValueBox(false)
  
  private def initializeIfNecessary(): Unit = initializedBox.mutate { initialized =>
    if(!initialized) {
      create()
    }
    
    true
  }
  
  override def accept(row: RenderableJsonRow): Unit = write(toJson(row))
  
  override def close(): Unit = commit(metadata)

  def withMetadata(newMetadata: JObject): AwsRowSink = copy(metadata = newMetadata)
  
  private[this] val uploadedSoFarBox: ValueBox[Int] = ValueBox(0)
  
  private[intake] def uploadedSoFar = uploadedSoFarBox.value
  
  private[this] val batch: Buffer[String] = new ArrayBuffer(batchSize)
  
  private[intake] def batchedSoFar: Int = uploadedSoFarBox.get(_ => batch.size)
  
  /**
   * The processor name to use for the runs database.
   */
  def processor: String = s"HDFS:${topic}"
  
  /**
   * Build the prefix key path for this dataset.
   */
  private val path: String = AwsRowSink.makePath(
      topic = topic,
      dataset = dataset,
      techType = techType,
      phenotype = phenotype,
      baseDir = baseDir)

  /**
   * Build a key for a given file.
   */
  private[intake] def toKey(fileName: String): String = s"${path}/${fileName}"
  
  /**
   * Delete all the data currently present for this dataset (if any).
   */
  def create(): Unit = uploadedSoFarBox.foreach { _ =>
    info(s"Preparing dataset ${path}...")

    //delete the dataset if it already exists
    delete()

    //initialize the batch now - cannot write before creating!
    batch.clear()
  }
  
  /**
   * Delete the dataset from HDFS.
   */
  def delete(): Unit = {
    val prefix = s"${path}/"
    
    info(s"Deleting '${path}'...")
    
    s3Client.deleteDir(prefix)
  }
  
  /**
   * Download the existing metadata JSON (if it exists). Can be useful for
   * comparing MD5 checksums, sources, etc.
   */
  def existingMetadata: Option[JObject] = {
    import Implicits._
    
    val metadataStringOpt: Option[String] = Try(s3Client.getAsString(toKey("metadata"))).getOrElse(None)
    
    metadataStringOpt.map(parse(_)).map(_ \ "Body").flatMap {
      case JString(body) => (Option(parse(body)).collect { case jobj: JObject => jobj })
      case _ => None
    }
  }

  /**
   * The metadata key can be written at any time. Sometimes when creating
   * a dataset the metadata information isn't known until later (e.g. after
   * all variants have been read).
   */
  def writeMetadata(metadata: JObject): Unit = {
    val body: String = compact(render(metadata))
    
    val metadataKey = toKey("metadata")

    info(s"Writing ${metadataKey}")

    s3Client.put(metadataKey, body, contentType = Some(S3Client.ContentType.ApplicationJson))
  }
  
  /**
   * Add an object to the batch as a JSON string.
   */
  def write(obj: JObject): Unit = {
    initializeIfNecessary()
    
    uploadedSoFarBox.foreach { _ =>
      //add the object to the batch as a JSON string (compacted)
      batch += compact(render(obj))
      
      //if the size reached the commit size, then commit it
      if(batch.size >= batchSize) {
        flush()
      }
    }
  }
  
  //TODO: How much padding?
  private[intake] def nextFileName(): String = f"part-${fileIds.next()}%05d-${uuid}.json"
  
  /**
   * Write the current batch to a part file.
   */
  def flush(): Unit = uploadedSoFarBox.foreach { _ =>
    if(batch.nonEmpty) {
      //the part file to write this data to
      val key = toKey(nextFileName())
      
      val body = batch.mkString("\n")

      //write the batched records to the bucket
      s3Client.put(key, body, contentType = Some(S3Client.ContentType.ApplicationJson))

      info(s"Wrote ${key}")

      //tally all the variants in the dataset
      uploadedSoFarBox.mutate(_ + batch.size)

      //wipe the batch and start fresh
      batch.clear()
    }
  }
  
  /**
   * Write the last batch and then write the dataset to the database.
   */
  private def commit(metadata: JObject): Unit = uploadedSoFarBox.foreach { _ =>
    flush()
  
    for {
      uploadedSoFar <- uploadedSoFarBox
    } {
      //don't write metadata if there are no records
      if(uploadedSoFar > 0) {
        writeMetadata(metadata)
        
        info(s"Committed ${uploadedSoFar} items to ${path}")
      } else {
        warn("No records; metadata not written!")
      }
    }
  }
  
  private[intake] def toJson(row: RenderableJsonRow): JObject = {
    JObject(row.jsonValues.to(List))
  }
}

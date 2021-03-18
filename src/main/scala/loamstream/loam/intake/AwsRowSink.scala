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
import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.Implicits
import software.amazon.awssdk.services.s3.model.NoSuchKeyException
import loamstream.util.AwsClient
import scala.util.Try

/**
 * @author clint
 * Dec 4, 2020
 */
object AwsRowSink {
  object Defaults {
    def fileIdSequence: Iterator[Long] = Sequence[Long]().iterator
  
    def randomUUID: String = UUID.randomUUID.toString
  
    def batchSize: Int = 300000
    
    def baseDir: Option[String] = None
  }
}

final case class AwsRowSink(
    topic: String,
    dataset: String,
    techType: Option[TechType],
    phenotype: Option[String],
    awsClient: AwsClient,
    batchSize: Int = AwsRowSink.Defaults.batchSize,
    baseDir: Option[String] = AwsRowSink.Defaults.baseDir,
    // :(
    yes: Boolean = false,
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
  
  override def close(): Unit = flush()

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
  private val path: String = {
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
    
    if(yes) {
      awsClient.deleteDir(prefix)
    }
  }
  
  /**
   * Download the existing metadata JSON (if it exists). Can be useful for
   * comparing MD5 checksums, sources, etc.
   */
  def existingMetadata: Option[JObject] = {
    import Implicits._
    
    val metadataStringOpt: Option[String] = Try(awsClient.getAsString(toKey("metadata"))).getOrElse(None)
    
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

    if(yes) {
      awsClient.put(metadataKey, body, contentType = Some(AwsClient.ContentType.ApplicationJson))
      
      //aws.put(metadataKey, body, ContentType="application/json") 
      
      /*self.s3.put_object(
          Bucket=self.bucket,
          Key=key,
          Body=body,
          ContentType="application/json",
      )*/
    }
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
      if(yes) {
        awsClient.put(key, body, contentType = Some(AwsClient.ContentType.ApplicationJson))
      }

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
  def commit(metadata: JObject): Unit = uploadedSoFarBox.foreach { _ =>
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
    JObject(row.jsonValues.toList)
  }
}
/**
 * import itertools
import json
import logging
import uuid

from botocore.errorfactory import ClientError

from .config import Config


class Dataset:
    """
    Take objects that can be written out in JSON format and batch them together
    to be uploaded to a location in S3 in part files.
    """

    def __init__(self, topic, name, batch_size=300000, yes=False):
        """
        Create connections to S3 and the MySQL database, then prepare for
        batched to start being written.
        """
        config = Config()

        self.s3, self.bucket = config.s3_client()
        self.count = itertools.count()
        self.uuid = str(uuid.uuid1())
        self.batch = None
        self.batch_size = batch_size
        self.topic = topic
        self.name = name
        self.yes = yes
        self.n = 0

    @property
    def processor(self):
        """
        The processor name to use for the runs database.
        """
        return "HDFS:%s" % self.topic

    @property
    def path(self):
        """
        Build the prefix key path for this dataset.
        """
        return '/'.join([self.topic, self.name])

    def key(self, name):
        """
        Build a key for a given file.
        """
        return '/'.join([self.path, name])

    def delete(self):
        """
        Delete the dataset from HDFS.
        """
        prefix = self.path + '/'

        # get the initial key listing
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, Delimiter='/')

        # fetch all the keys in this dataset that already exist
        while resp.get('KeyCount', 0) > 0:
            logging.info('Deleting %d existing keys from %s...', resp.get('KeyCount'), self.path)

            if self.yes:
                keys = [{'Key': obj['Key']} for obj in resp['Contents']]

                self.s3.delete_objects(
                    Bucket=self.bucket,
                    Delete={
                        'Objects': keys,
                        'Quiet': True
                    },
                )

            # bugger out if there are not more keys
            if not resp['IsTruncated']:
                break

            # get the next set of keys
            resp = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix,
                Delimiter='/',
                ContinuationToken=resp['NextContinuationToken'],
            )

    def create(self):
        """
        Delete all the data currently present for this dataset (if any).
        """
        logging.info('Preparing dataset %s...', self.path)

        # delete the dataset if it already exists
        self.delete()

        # initialize the batch now - cannot write before creating!
        self.batch = []

    def existing_metadata(self):
        """
        Download the existing metadata JSON (if it exists). Can be useful for
        comparing MD5 checksums, sources, etc.
        """
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=self.key('metadata'))
            if not obj:
                return None

            # parse the entire contents of the key as JSON
            return json.loads(obj['Body'].read())
        except ClientError:
            return {}

    def write_metadata(self, metadata):
        """
        The metadata key can be written at any time. Sometimes when creating
        a dataset the metadata information isn't known until later (e.g. after
        all variants have been read).
        """
        body = json.dumps(metadata).encode()
        key = self.key('metadata')

        logging.info('Writing %s...', key)

        if self.yes:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType='application/json',
            )

    def write(self, obj):
        """
        Add an object to the batch as a JSON string.
        """
        assert self.batch is not None, "Cannot write to the dataset before calling .create()!"

        # add the object to the batch as a JSON string (compacted)
        self.batch.append(json.dumps(obj).encode())

        # if the size reached the commit size, then commit it
        if len(self.batch) >= self.batch_size:
            self.flush()

    def flush(self):
        """
        Write the current batch to a part file.
        """
        if len(self.batch) == 0:
            return

        # the part file to write this data to
        key = self.key('part-%05d-%s.json' % (next(self.count), self.uuid))
        body = b'\n'.join(self.batch)

        # write the batched records to the bucket
        if self.yes:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType='application/json',
            )

        logging.info('Wrote %s', key)

        # tally all the variants in the dataset
        self.n += len(self.batch)

        # wipe the batch and start fresh
        self.batch.clear()

    def commit(self, metadata):
        """
        Write the last batch and then write the dataset to the database.
        """
        self.flush()

        # don't write metadata if there are no records
        if self.n > 0:
            self.write_metadata(metadata)
            logging.info('Committed %d items to %s', self.n, self.path)
        else:
            logging.warning('No records; metadata not written!')

    def touch(self):
        """
        This does nothing now.
        """
        pass
 * 
 */

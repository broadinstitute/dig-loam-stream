package loamstream.loam.intake

import java.util.UUID

import org.json4s.JsonAST.JValue
import org.scalatest.FunSuite

import AwsRowSinkTest.MockS3Client
import loamstream.loam.intake.dga.Json
import loamstream.util.S3Client
import loamstream.util.S3Client.ContentType

import scala.collection.compat._

/**
 * @author clint
 * Dec 7, 2020
 */
final class AwsRowSinkTest extends FunSuite {
  test("toKey - no tech or phenotype") {
    val sink = AwsRowSink(
        topic = "some-topic", 
        dataset = "some-name",
        techType = None,
        phenotype = None,
        batchSize = 42,
        s3Client = MockS3Client.apply("some-bucket"))
        
    assert(sink.toKey("lalala.json") === "some-topic/some-name/lalala.json")
    assert(sink.toKey("") === "some-topic/some-name/")
    assert(sink.toKey("foo/bar/baz") === "some-topic/some-name/foo/bar/baz")
  }
  
  test("toKey - with tech and phenotype") {
    def doTest(techType: TechType, phenotype: String): Unit = {
      val sink = AwsRowSink(
          topic = "some-topic", 
          dataset = "some-name",
          techType = Some(techType),
          phenotype = Some(phenotype),
          batchSize = 42,
          s3Client = MockS3Client.apply("some-bucket"))
          
      assert(sink.toKey("lalala.json") === s"some-topic/${techType.name}/some-name/${phenotype}/lalala.json")
      assert(sink.toKey("") === s"some-topic/${techType.name}/some-name/${phenotype}/")
      assert(sink.toKey("foo/bar/baz") === s"some-topic/${techType.name}/some-name/${phenotype}/foo/bar/baz")
    }
    
    import TechType._
    
    doTest(Gwas, "foo")
    doTest(ExChip, "bar")
    doTest(ExSeq, "baz")
    doTest(Fm, "blerg")
    doTest(IChip, "zerg")
    doTest(Wgs, "nerg")
  }
  
  private def randomUUID: String = UUID.randomUUID.toString
  
  test("nextFileName") {
    val uuid = randomUUID
    
    val sink = AwsRowSink(
        topic = "some-topic", 
        dataset = "some-name", 
        batchSize = 42,
        techType = None,
        phenotype = None,
        s3Client = MockS3Client.apply("some-bucket"),
        fileIds = Iterator(1,2,3,4,5),
        uuid = uuid)
        
    val fileNames = (1 to 5).map(_ => sink.nextFileName())
    
    val expected = Seq(
        s"part-00001-${uuid}.json",
        s"part-00002-${uuid}.json",
        s"part-00003-${uuid}.json",
        s"part-00004-${uuid}.json",
        s"part-00005-${uuid}.json")
        
    assert(fileNames === expected)
  }

  private def toJsonRow(row: RenderableRow): RenderableJsonRow = new RenderableJsonRow {
    override def jsonValues: Seq[(String, JValue)] = row.headers.zip(row.values.map(Json.toJValue(_)))
  }
  
  test("accept / write / flush") {
    val headers = Seq("X", "Y", "Z")
    
    val rows = Seq(
        LiteralRow(headers = headers, values = Seq("4", "3", "2")),
        LiteralRow(headers = headers, values = Seq("z", "x", "c")),
        LiteralRow(headers = headers, values = Seq("q", "w", "e")),
        LiteralRow(headers = headers, values = Seq("f", "o", "o"))).map(toJsonRow)
        
    val client = MockS3Client.apply("some-bucket")
        
    val uuid = randomUUID 
    
    val sink = AwsRowSink(
        topic = "some-topic", 
        dataset = "some-name", 
        batchSize = 2,
        techType = None,
        phenotype = None,
        s3Client = client,
        yes = true,
        fileIds = Iterator(2, 4, 6, 8),
        uuid = uuid)
        
    assert(client.isEmpty)
    assert(sink.uploadedSoFar === 0)
    assert(sink.batchedSoFar === 0)
        
    sink.accept(rows(0))
    
    assert(client.isEmpty)
    assert(sink.uploadedSoFar === 0)
    assert(sink.batchedSoFar === 1)
    
    sink.accept(rows(1))
    
    val newline = '\n'
    
    val expected0 = Map(
      s"some-bucket/some-topic/some-name/part-00002-${uuid}.json" ->
        AwsRowSinkTest.MockValue(
            s"""{"X":"4","Y":"3","Z":"2"}${newline}{"X":"z","Y":"x","Z":"c"}""", 
            Some(S3Client.ContentType.ApplicationJson)))
      
    
    assert(client.data === expected0)
    assert(sink.uploadedSoFar === 2)
    assert(sink.batchedSoFar === 0)
    
    sink.accept(rows(2))
    
    assert(client.data === expected0)
    assert(sink.uploadedSoFar === 2)
    assert(sink.batchedSoFar === 1)
    
    sink.accept(rows(3))
    
    val expected1 = expected0 + (
      s"some-bucket/some-topic/some-name/part-00004-${uuid}.json" -> 
        AwsRowSinkTest.MockValue(
            s"""{"X":"q","Y":"w","Z":"e"}${newline}{"X":"f","Y":"o","Z":"o"}""", 
            Some(S3Client.ContentType.ApplicationJson)))
        
    assert(client.data === expected1)
    assert(sink.uploadedSoFar === 4)
    assert(sink.batchedSoFar === 0)
  }
  
  test("flush") {
    val headers = Seq("X", "Y", "Z")
    
    val rows = Seq(
        LiteralRow(headers = headers, values = Seq("4", "3", "2")),
        LiteralRow(headers = headers, values = Seq("z", "x", "c")),
        LiteralRow(headers = headers, values = Seq("q", "w", "e")),
        LiteralRow(headers = headers, values = Seq("f", "o", "o"))).map(toJsonRow)
        
    val client = MockS3Client.apply("some-bucket")
        
    val uuid = randomUUID 
    
    val sink = AwsRowSink(
        topic = "some-topic", 
        dataset = "some-name",
        techType = None,
        phenotype = None,
        batchSize = 2,
        s3Client = client,
        yes = true,
        fileIds = Iterator(2, 4, 6, 8),
        uuid = uuid)
        
    assert(client.isEmpty)
        
    sink.accept(rows(0))
    
    assert(client.isEmpty)
    
    sink.flush()
    
    val json = Some(S3Client.ContentType.ApplicationJson)
    
    val expected0 = Map(
      s"some-bucket/some-topic/some-name/part-00002-${uuid}.json" -> 
        AwsRowSinkTest.MockValue(s"""{"X":"4","Y":"3","Z":"2"}""", json))
    
    assert(client.data === expected0)
  }
}

object AwsRowSinkTest {
  final case class MockValue(value: String, contentType: Option[ContentType])
  
  object MockS3Client {
    def empty(bucket: String): MockS3Client = new MockS3Client(bucket)
  }
  
  final case class MockS3Client(bucket: String, initialData: Map[String, String] = Map.empty) extends S3Client {
    import loamstream.util.Maps.Implicits._
    
    var data: Map[String, MockValue] = initialData.strictMapValues(MockValue(_, None))
    
    def isEmpty: Boolean = data.isEmpty
    
    override def list(prefix: String, delimiter: String): Seq[String] = {
      //TODO: Delim?
      data.keys.to(Seq).sorted.filter(_.startsWith(prefix))
    }
  
    override def deleteDir(key: String): Unit = {
      data = data.strictFilterKeys(!_.startsWith(key))
    }
  
    override def put(key: String, body: String, contentType: Option[ContentType] = None): Unit = {
      data += (s"${bucket}/${key}" -> MockValue(body, contentType))
    }
  
    override def getAsString(key: String): Option[String] = data.get(s"${bucket}/${key}").map(_.value)
  }
    
}

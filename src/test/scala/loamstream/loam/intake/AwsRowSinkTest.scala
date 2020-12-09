package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.util.AwsClient
import loamstream.util.AwsClient.ContentType
import loamstream.util.Maps
import AwsRowSinkTest.MockAwsClient
import java.util.UUID

/**
 * @author clint
 * Dec 7, 2020
 */
final class AwsRowSinkTest extends FunSuite {
  test("toKey") {
    val sink = AwsRowSink(
        topic = "some-topic", 
        name = "some-name", 
        batchSize = 42,
        awsClient = MockAwsClient.apply("some-bucket"))
        
    assert(sink.toKey("lalala.json") === "some-topic/some-name/lalala.json")
    assert(sink.toKey("") === "some-topic/some-name/")
    assert(sink.toKey("foo/bar/baz") === "some-topic/some-name/foo/bar/baz")
  }
  
  private def randomUUID: String = UUID.randomUUID.toString
  
  test("nextFileName") {
    val uuid = randomUUID
    
    val sink = AwsRowSink(
        topic = "some-topic", 
        name = "some-name", 
        batchSize = 42,
        awsClient = MockAwsClient.apply("some-bucket"),
        fileIds = Iterator(1,2,3,4,5),
        uuid = uuid)
        
    val fileNames = (1 to 5).map(_ => sink.nextFileName())
    
    val expected = Seq(
        s"part-001-${uuid}.json",
        s"part-002-${uuid}.json",
        s"part-003-${uuid}.json",
        s"part-004-${uuid}.json",
        s"part-005-${uuid}.json")
        
    assert(fileNames === expected)
  }
  
  test("accept / write / flush") {
    val headers = Seq("X", "Y", "Z")
    
    val rows = Seq(
        LiteralRow(headers = headers, values = Seq("4", "3", "2")),
        LiteralRow(headers = headers, values = Seq("z", "x", "c")),
        LiteralRow(headers = headers, values = Seq("q", "w", "e")),
        LiteralRow(headers = headers, values = Seq("f", "o", "o")))
        
    val client = MockAwsClient.apply("some-bucket")
        
    val uuid = randomUUID 
    
    val sink = AwsRowSink(
        topic = "some-topic", 
        name = "some-name", 
        batchSize = 2,
        awsClient = client,
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
      s"some-bucket/some-topic/some-name/part-002-${uuid}.json" -> 
        AwsRowSinkTest.MockValue(s"""{"X":"4","Y":"3","Z":"2"}${newline}{"X":"z","Y":"x","Z":"c"}""", Some(AwsClient.ContentType.ApplicationJson)))
    
    assert(client.data === expected0)
    assert(sink.uploadedSoFar === 2)
    assert(sink.batchedSoFar === 0)
    
    sink.accept(rows(2))
    
    assert(client.data === expected0)
    assert(sink.uploadedSoFar === 2)
    assert(sink.batchedSoFar === 1)
    
    sink.accept(rows(3))
    
    val expected1 = expected0 + (
      s"some-bucket/some-topic/some-name/part-004-${uuid}.json" -> 
        AwsRowSinkTest.MockValue(s"""{"X":"q","Y":"w","Z":"e"}${newline}{"X":"f","Y":"o","Z":"o"}""", Some(AwsClient.ContentType.ApplicationJson)))
        
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
        LiteralRow(headers = headers, values = Seq("f", "o", "o")))
        
    val client = MockAwsClient.apply("some-bucket")
        
    val uuid = randomUUID 
    
    val sink = AwsRowSink(
        topic = "some-topic", 
        name = "some-name", 
        batchSize = 2,
        awsClient = client,
        yes = true,
        fileIds = Iterator(2, 4, 6, 8),
        uuid = uuid)
        
    assert(client.isEmpty)
        
    sink.accept(rows(0))
    
    assert(client.isEmpty)
    
    sink.flush()
    
    val json = Some(AwsClient.ContentType.ApplicationJson)
    
    val expected0 = Map(
      s"some-bucket/some-topic/some-name/part-002-${uuid}.json" -> 
        AwsRowSinkTest.MockValue(s"""{"X":"4","Y":"3","Z":"2"}""", json))
    
    assert(client.data === expected0)
  }
}

object AwsRowSinkTest {
  final case class MockValue(value: String, contentType: Option[ContentType])
  
  object MockAwsClient {
    def empty(bucket: String): MockAwsClient = new MockAwsClient(bucket)
  }
  
  final case class MockAwsClient(bucket: String, initialData: Map[String, String] = Map.empty) extends AwsClient {
    import Maps.Implicits._
    
    var data: Map[String, MockValue] = initialData.strictMapValues(MockValue(_, None))
    
    def isEmpty: Boolean = data.isEmpty
    
    override def list(prefix: String, delimiter: String): Seq[String] = {
      //TODO: Delim?
      data.keys.toSeq.sorted.filter(_.startsWith(prefix))
    }
  
    override def deleteDir(key: String): Unit = {
      data = data.filterKeys(!_.startsWith(key))
    }
  
    override def put(key: String, body: String, contentType: Option[ContentType] = None): Unit = {
      data += (s"${bucket}/${key}" -> MockValue(body, contentType))
    }
  
    override def getAsString(key: String): Option[String] = data.get(s"${bucket}/${key}").map(_.value)
  }
    
}

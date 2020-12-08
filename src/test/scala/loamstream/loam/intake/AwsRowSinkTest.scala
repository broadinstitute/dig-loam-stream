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
  
  test("nextFileName") {
    val uuid = UUID.randomUUID.toString
    
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
  
  test("write / accept") {
    def doTest(put: AwsRowSink => RenderableRow => Unit): Unit = {
      val headers = Seq("X", "Y", "Z")
      
      val rows = Seq(
          LiteralRow(headers = headers, values = Seq("4", "3", "2")),
          LiteralRow(headers = headers, values = Seq("z", "x", "c")),
          LiteralRow(headers = headers, values = Seq("q", "w", "e")))
          
      val client = MockAwsClient.apply("some-bucket")
          
      val sink = AwsRowSink(
          topic = "some-topic", 
          name = "some-name", 
          batchSize = 2,
          awsClient = client)
          
      assert(client.isEmpty)
          
      put(sink)(rows(0))
      
      assert(client.isEmpty)
    }
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

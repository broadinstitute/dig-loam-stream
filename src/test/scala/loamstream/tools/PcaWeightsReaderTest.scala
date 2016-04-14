package loamstream.tools

import org.scalatest.FunSuite
import java.nio.file.Paths
import scala.io.Source

/**
 * @author clint
 * date: Apr 11, 2016
 */
final class PcaWeightsReaderTest extends FunSuite {
  test("read() should work on a good file with blank lines") {
    val weightsFile = Paths.get("src/test/resources/data/pca/1kg_v3_5k_sites.pca.snpwts.small")
    
    val result = PcaWeightsReader.read(weightsFile)
    
    val expected = Map(
      "rs11260566" -> Seq(1.409, 0.583, -0.403),
      "rs2803348" -> Seq(0.282, -0.440, 0.579),
      "rs16824588" -> Seq(0.315, -0.586, 0.965),
      "rs1878745" -> Seq(-1.497, 1.655, -0.544))
      
    assert(result === expected) 
  }
  
  test("read() should work on a file with some bad lines") {
    val weightsFile = Paths.get("src/test/resources/data/pca/1kg_v3_5k_sites.pca.snpwts.small.some-bad-lines")
    
    val result = PcaWeightsReader.read(weightsFile)
    
    val expected = Map(
      "rs11260566" -> Seq(1.409, 0.583, -0.403),
      "rs16824588" -> Seq(0.315, -0.586, 0.965))
      
    assert(result === expected) 
  }
  
  test("read() should work on a file with only blank lines") {
    val result = PcaWeightsReader.read(Source.fromIterable("\n\n\n\n"))
    
    assert(result === Map.empty) 
  }
  
  test("read() should work on an empty file") {
    val result = PcaWeightsReader.read(Source.fromIterable(""))
    
    assert(result === Map.empty) 
  }
}
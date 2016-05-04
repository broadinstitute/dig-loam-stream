package loamstream.tools.core

import org.scalatest.FunSuite
import loamstream.model.kinds.LSpecificKind
import loamstream.model.ToolSpec

/**
 * @author clint
 * date: May 4, 2016
 */
final class CoreToolSpecTest extends FunSuite {
  import CoreToolSpec._
  import CoreStoreSpec._
  import StoreOps._
  
  private val kind = LSpecificKind("foo")
  
  test("unaryTool") {
    assert(unaryTool(kind, vcfFile ~> vdsFile) == ToolSpec(kind, Seq(vcfFile), vdsFile))
  }
  
  test("binaryTool") {
    val expected = ToolSpec(kind, Seq(vcfFile, vdsFile), pcaWeightsFile)
    
    assert(binaryTool(kind, (vcfFile, vdsFile) ~> pcaWeightsFile) == expected)
  }
  
  test("nAryTool") {
    val expected = ToolSpec(kind, Seq(vcfFile, vdsFile), pcaWeightsFile)
    
    assert(nAryTool(kind, Seq(vcfFile, vdsFile) ~> pcaWeightsFile) == expected)
  }
}
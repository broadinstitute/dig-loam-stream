package loamstream.tools.core

import org.scalatest.FunSuite
import loamstream.model.StoreSpec
import loamstream.model.ToolSpec
import loamstream.model.kinds.LAnyKind

/**
 * @author clint
 * date: Apr 29, 2016
 */
final class StoreOpsTest extends FunSuite {
  import CoreStore._
  import StoreOps._
  
  test("~>") {
    assert(vcfFile ~> vdsFile === UnarySig(vcfFile, vdsFile))
    
    assert(UnarySig(vcfFile, vdsFile).toNarySig === NarySig(Seq(vcfFile), vdsFile))
        
    assert((vcfFile, sampleIdsFile) ~> vdsFile === BinarySig((vcfFile, sampleIdsFile), vdsFile))
    
    assert(BinarySig((vcfFile, sampleIdsFile), vdsFile).toNarySig === NarySig(Seq(vcfFile, sampleIdsFile), vdsFile))
    
    val narySig = NarySig(Seq(vcfFile, sampleIdsFile, singletonsFile), vdsFile)
    
    assert(Seq(vcfFile, sampleIdsFile, singletonsFile) ~> vdsFile === narySig)
    
    assert(narySig.toNarySig === narySig)
  }
  
  test("toNarySig") {
    assert((vcfFile ~> vdsFile).toNarySig == NarySig(Seq(vcfFile), vdsFile))
    
    assert(((vcfFile, vdsFile) ~> pcaWeightsFile).toNarySig == NarySig(Seq(vcfFile, vdsFile), pcaWeightsFile))
    
    val narySig = NarySig(Seq(vcfFile, vdsFile), pcaWeightsFile)
    
    assert(narySig.toNarySig == narySig)
  }
  
  test("as()") {
    def someToolSpec(input: StoreSpec, output: StoreSpec): ToolSpec = {
      ToolSpec(LAnyKind, Seq(input), output)
    }
    
    val toolSpec = (vcfFile ~> vdsFile).as(someToolSpec)
    
    assert(toolSpec == ToolSpec(LAnyKind, Seq(vcfFile), vdsFile))
  }
}
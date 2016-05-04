package loamstream.tools.core

import org.scalatest.FunSuite

/**
 * @author clint
 * date: Apr 29, 2016
 */
final class StoreOpsTest extends FunSuite {
  test("~>") {
    import CoreStore._
    import StoreOps._
    
    assert(vcfFile ~> vdsFile === UnarySig(vcfFile, vdsFile))
    
    assert(UnarySig(vcfFile, vdsFile).toNarySig === NarySig(Seq(vcfFile), vdsFile))
        
    assert((vcfFile, sampleIdsFile) ~> vdsFile === BinarySig((vcfFile, sampleIdsFile), vdsFile))
    
    assert(BinarySig((vcfFile, sampleIdsFile), vdsFile).toNarySig === NarySig(Seq(vcfFile, sampleIdsFile), vdsFile))
    
    val narySig = NarySig(Seq(vcfFile, sampleIdsFile, singletonsFile), vdsFile)
    
    assert(Seq(vcfFile, sampleIdsFile, singletonsFile) ~> vdsFile === narySig)
    
    assert(narySig.toNarySig === narySig)
  }
}
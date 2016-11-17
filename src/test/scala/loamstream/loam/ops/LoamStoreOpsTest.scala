package loamstream.loam.ops

import loamstream.compiler.LoamEngine
import loamstream.compiler.LoamPredef._
import loamstream.loam.ops.StoreType.BIM
import loamstream.loam.ops.filters.StoreFieldFilter
import loamstream.loam.{LoamProjectContext, LoamScript, LoamScriptContext}
import org.scalatest.FunSuite

/** Test Loam store ops */
class LoamStoreOpsTest extends FunSuite {
  test("LoamStore.filter() unstringed") {
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty)
    val variants = store[BIM]
    val variantsUnplaced = variants.filter(StoreFieldFilter.isUndefined(BIM.chr))
    val variantsOnChr7 = variants.filter(BIM.chr)(_ == 7)
  }
  test("LoamStore.filter()") {
    val script = LoamScript("filter",
      """
        |val variants = store[BIM]
        |val variantsUnplaced = variants.filter(StoreFieldFilter.isUndefined(BIM.chr))
        |val variantsOnChr7 = variants.filter(BIM.chr)(_ == 7)
      """.stripMargin)
    val engine = LoamEngine.default()
    val result = engine.run(script)
  }
}

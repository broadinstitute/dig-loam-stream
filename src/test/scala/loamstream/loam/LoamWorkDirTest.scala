package loamstream.loam

import java.nio.file.Paths

import loamstream.compiler.LoamPredef
import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 10/17/2016.
  */
class LoamWorkDirTest extends FunSuite {
  test("Check work dirs and file paths are correct") {
    import LoamPredef._
    import LoamCmdTool._
    implicit val scriptContext = new LoamScriptContext(LoamProjectContext.empty)
    val store1 = store[VCF].from("file1.vcf")
    val store2 = store[VCF].from("file2.vcf")
    val tool1 = cmd"yo $store1 $store2"
    assert(store1.path === "file1.vcf")
    assert(store1.path === "file2.vcf")
    assert(tool1.workDirOpt.get.normalize() === Paths.get(".").normalize())
  }
}


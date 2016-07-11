package loamstream

import java.nio.file.Path
import java.nio.file.Paths

import scala.io.Source

import org.apache.commons.io.FileUtils
import org.scalatest.FunSuite

import loamstream.util.Hit
import loamstream.util.LoamFileUtils

/**
 * Created by kyuksel on 2/29/2016.
 */
final class HailSingletonEndToEndTest extends FunSuite with LoamTestHelpers {

  //NB: Ignored, since it depends on an external tool
  ignore("Singletons are successfully counted using Hail") {
    import scala.concurrent.ExecutionContext.Implicits.global

    val hailSingletonFilePath = Paths.get("target/singletons.tsv")
    val hailVdsDir = Paths.get("target/hail.vds")

    deleteBeforeAndAfter(hailSingletonFilePath, hailVdsDir) {
      val executable = compileFile("src/test/resources/loam/singletons-via-hail.loam")

      val results = run(executable)

      assert(results.size == 2)

      //TODO: More-explicit test for better message on failures.
      results.values.forall {
        case Hit(r) => r.isSuccess
        case _      => false
      }

      val source = Source.fromFile(hailSingletonFilePath.toFile)

      val singletonCounts = LoamFileUtils.enclosed(source)(_.getLines.toList)

      assert(singletonCounts.size == 101)
      assert(singletonCounts.head == "SAMPLE\tSINGLETONS")
      assert(singletonCounts.tail.head == "C1046::HG02024\t0")
      assert(singletonCounts.last == "HG00629\t0")
    }
  }

  private def deleteBeforeAndAfter[A](files: Path*)(f: => A): A = {
    def deleteQuietly(path: Path): Unit = FileUtils.deleteQuietly(path.toFile)

    try {
      // Make sure to not mistakenly use an output file from a previous run, if any
      files.foreach(deleteQuietly)

      f
    } finally {
      // Make sure to not mistakenly use an output file from a previous run, if any
      files.foreach(deleteQuietly)
    }
  }
}

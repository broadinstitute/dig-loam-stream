package loamstream.apps

import org.scalatest.FunSuite

import loamstream.conf.LoamConfig
import loamstream.loam.LoamSyntax
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamGraph
import loamstream.compiler.LoamEngine
import loamstream.conf.LsSettings
import loamstream.conf.UgerConfig
import loamstream.conf.ExecutionConfig
import loamstream.loam.intake.ColumnName
import java.nio.file.Path
import java.nio.file.Files.exists
import loamstream.util.{ Files => LFiles }
import loamstream.drm.DrmSystem
import loamstream.cli.Conf
import loamstream.cli.Intent
import loamstream.util.Processes
import loamstream.util.Loggable

/**
 * @author clint
 * Oct 6, 2020
 */
final class TenNativeJobsOnUgerTest extends FunSuite with Loggable {
  import loamstream.IntegrationTestHelpers.withWorkDirUnderTarget
  import loamstream.IntegrationTestHelpers.makeGraph
  import TenNativeJobsOnUgerTest.loamCode
  import loamstream.IntegrationTestHelpers.path
    
  test("Ten native jobs on Uger") {
    
    withWorkDirUnderTarget() { workDir =>
      val noRetries = ExecutionConfig.default.copy(maxRunsPerJob = 1)

      val n = 10
      
      val loamFile = workDir.resolve("NJobs.scala")
      
      LFiles.writeTo(loamFile)(loamCode(n, workDir))

      val loamConf = workDir.resolve("loamstream.conf")
      
      val confContents = """|loamstream { 
                            |  uger { } 
                            |  execution { 
                            |    maxRunsPerJob = 1 
                            |  }
                            |}""".stripMargin
      
      LFiles.writeTo(loamConf)(confContents)
      
      val tokens = Seq("java", "-jar", "target/scala-2.12/loamstream-assembly-1.4-SNAPSHOT.jar",
                       "--backend", "uger",
                       "--conf", loamConf.toString,
                       "--loams", loamFile.toString)
      
      val runResult = Processes.runSync(tokens)()
      
      assert(runResult.isSuccess)
      
      assert(runResult.flatMap(_.tryAsSuccess).isSuccess)

      def toOutputFilePath(i: Int): Path = {
        workDir.resolve("loam_out").resolve(s"out-${i}.tsv").toAbsolutePath
      }
      
      val expectedOutputFiles: Set[Path] = (1 to n).map(toOutputFilePath).toSet
      
      val actualOutputFiles: Set[Path] = {
        workDir.toFile.listFiles.filter(_.getName.endsWith(".tsv")).map(_.toPath).toSet
      }
      
      assert(actualOutputFiles === expectedOutputFiles)
      
      import TenNativeJobsOnUgerTest.ColumnNames
      
      val mungedOutputColumns: Seq[ColumnName] = {
        import ColumnNames._
  
        Seq(BLERG, ZERG, NERG, GLERG, FLERG, BLERG, ZERG, NERG, GLERG, FLERG)
      }
      
      for {
        i <- 1 to n
      } {
        val outputFile = toOutputFilePath(i)
        
        assert(exists(outputFile))
        
        val mungedOutputColumn = mungedOutputColumns(i)
        
        val expectedOutput = s"""|${ColumnNames.FOO.name} ${ColumnNames.BAR.name} ${mungedOutputColumn.name}
                                 |a b 43
                                 |x y 100
                                 |la la 124""".stripMargin
                                 
        assert(
            LFiles.readFrom(outputFile) === expectedOutput,
            s"Unexpected contents in ${outputFile.toAbsolutePath}")                                 
      }
    }
  }
}

object TenNativeJobsOnUgerTest {
  private object ColumnNames {
      val FOO = ColumnName("FOO")
      val BAR = ColumnName("BAR")
      val BAZ = ColumnName("BAZ")
      val BLERG = ColumnName("BLERG")
      val ZERG = ColumnName("ZERG")
      val NERG = ColumnName("NERG")
      val GLERG = ColumnName("GLERG")
      val FLERG = ColumnName("FLERG")
    }
  
  private def loamCode(n: Int, workDir: Path): String = {
s"""
object NJobs extends loamstream.LoamFile {
  import loamstream.loam.intake.IntakeSyntax._
  
  val csvData = s\"\"\"|${ColumnNames.FOO.name} ${ColumnNames.BAR.name} ${ColumnNames.BAZ.name}
                    |a b 42
                    |x y 99
                    |la la 123\"\"\".stripMargin
  
  val source: CsvSource = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
  
  object NoopFlipDetector extends FlipDetector {
    override def isFlipped(variantId: String): Boolean = false
  }

  object ColumnNames {
    val FOO = ColumnName("FOO")
    val BAR = ColumnName("BAR")
    val BAZ = ColumnName("BAZ")
    val BLERG = ColumnName("BLERG")
    val ZERG = ColumnName("ZERG")
    val NERG = ColumnName("NERG")
    val GLERG = ColumnName("GLERG")
    val FLERG = ColumnName("FLERG")
  }
  
  val destColumns = {
    import ColumnNames._
  
    Seq(BLERG, ZERG, NERG, GLERG, FLERG)
  }
  
  for {
    (i, mungedDestColumnName) <- (1 to ${n}).zip(destColumns ++ destColumns)
  } {
    val rowDef = { 
      RowDef(
        varIdDef = ColumnDef(ColumnNames.FOO),
        otherColumns = Seq(ColumnDef(ColumnNames.BAR), 
                           ColumnDef(mungedDestColumnName, ColumnNames.BAZ.asInt.map(_ + 1))))
      }.from(source)
  
      val dest = store(path("${workDir}").resolve("loam_out").resolve(s"out-$${i}.tsv"))
  
    drm {
      produceCsv(dest).from(rowDef).using(NoopFlipDetector).tag(s"munge-csv-data-$${i}")
    }
  }
}""".trim
  }
}

package loamstream

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

/**
 * @author clint
 * Oct 6, 2020
 */
final class TenNativeJobsOnUgerTest extends FunSuite {
  import IntegrationTestHelpers.withWorkDirUnderTarget
  import IntegrationTestHelpers.makeGraph
  import TenNativeJobsOnUgerTest.loamCode
  import IntegrationTestHelpers.path
    
  test("Ten native jobs on Uger") {
    
    
    withWorkDirUnderTarget() { workDir =>
      val noRetries = ExecutionConfig.default.copy(maxRunsPerJob = 1)
      
      val config: LoamConfig = {
        val empty: LoamConfig = LoamConfig.fromString("{}").get
        
        val ugerDefaults = UgerConfig()
        
        empty.copy(ugerConfig = Some(ugerDefaults), executionConfig = noRetries, drmSystem = Some(DrmSystem.Uger))
      }
      
      val n = 10
      
      val graph = makeGraph(config)(loamCode(n, workDir)(_))
      
      val loamEngine = LoamEngine.default(config, LsSettings.noCliConfig)
      
      val results = loamEngine.run(graph, noRetries)
      
      assert(results.size === n)
      
      import TenNativeJobsOnUgerTest.ColumnNames
      
      val mungedOutputColumns: Seq[ColumnName] = {
        import ColumnNames._
  
        Seq(BLERG, ZERG, NERG, GLERG, FLERG, BLERG, ZERG, NERG, GLERG, FLERG)
      }
      
      for {
        i <- 1 to n
      } {
        val outputFile = workDir.resolve("loam_out").resolve(s"out-${i}.tsv")
        
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
  
  private def loamCode(n: Int, workDir: Path)(implicit context: LoamScriptContext): Unit = {
    import LoamSyntax._
    import IntakeSyntax._

    val csvData = s"""|${ColumnNames.FOO.name} ${ColumnNames.BAR.name} ${ColumnNames.BAZ.name}
                      |a b 42
                      |x y 99
                      |la la 123""".stripMargin
    
    val source: CsvSource = CsvSource.fromString(csvData, CsvSource.Formats.spaceDelimitedWithHeader)
    
    object NoopFlipDetector extends FlipDetector {
      override def isFlipped(variantId: String): Boolean = false
    }
    
    val destColumns = {
      import ColumnNames._
  
      Seq(BLERG, ZERG, NERG, GLERG, FLERG)
    }
  
    for {
      (i, mungedDestColumnName) <- (1 to n).zip(destColumns ++ destColumns)
    } {
      val rowDef = { 
        RowDef(
          varIdDef = ColumnDef(ColumnNames.FOO),
          otherColumns = Seq(ColumnDef(ColumnNames.BAR), 
                             ColumnDef(mungedDestColumnName, ColumnNames.BAZ.asInt.map(_ + 1))))
        }.from(source)
    
        val dest = store(workDir.resolve("loam_out").resolve(s"out-${i}.tsv"))
    
      drm {
        produceCsv(dest).from(rowDef).using(NoopFlipDetector).tag(s"munge-csv-data-${i}")
      }
    }
  }
}

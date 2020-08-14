package loamstream

import org.scalatest.FunSuite
import loamstream.loam.intake.IntakeSyntax
import loamstream.loam.LoamSyntax
import loamstream.util.Files
import java.nio.file.Path
import loamstream.util.jvm.JvmArgs
import loamstream.util.ProcessLoggers
import loamstream.util.Loggable

/**
 * @author clint
 * Aug 13, 2020
 */
final class SimpleIntakeOnUgerTest extends FunSuite with Loggable {
  test("run one intake job on Uger") {
    IntegrationTestHelpers.withWorkDirUnderTarget(deleteWhenDone = false) { workDir =>
      val loamCode = """
object IntakeOnUger extends loamstream.LoamFile {
  val tab = "\t"
    
  val inputTsv = s\"\"\"|VARID${tab}X${tab}Y
                     |1_1_A_T${tab}abc${tab}42
                     |1_2_G_C${tab}xyz${tab}99\"\"\".trim.stripMargin
  
  val dest = store(s"${workDir}/out.tsv")
  
  val flipDetector: FlipDetector = new FlipDetector.Default(
    referenceDir = path("/humgen/diabetes2/users/mvg/portal/scripts/reference"),
    isVarDataType = true,
    pathTo26kMap = path("/humgen/diabetes2/users/mvg/portal/scripts/26k_id.map"))
  
  val source = CsvSource.fromString(inputTsv, CsvSource.Formats.tabDelimitedWithHeader)
  
  object ColumnNames {
    val varId = ColumnName("VARID")
    val x = ColumnName("X")
    val y = ColumnName("Y")
  }
  
  val rowDef = {
    UnsourcedRowDef(
      varIdDef = ColumnDef(ColumnNames.varId), 
      otherColumns = Seq(ColumnDef(ColumnNames.x), ColumnDef(ColumnNames.y))).from(source)
  }

  drm {
    produceCsv(dest).from(rowDef).using(flipDetector).tag("fake-stub-tag-name")
  }
}"""
  
      val loamFile: Path = workDir.resolve("IntakeOnUger.scala")
      
      Files.writeTo(loamFile)(loamCode)
      
      val jvmArgs = JvmArgs()
      
      val lsJar = "target/scala-2.12/loamstream-assembly-1.4-SNAPSHOT.jar"
      
      val commandToRun: Seq[String] = (jvmArgs.javaBinary.toString +: jvmArgs.jvmArgs) ++ Seq("-jar", lsJar) ++ Seq("--backend", "uger", "--loams", loamFile.toString)
      
      info(s"Running LS: '${commandToRun.mkString(" ")}'") 
      
      {
        import scala.sys.process._
        
        val processLogger = new ProcessLoggers.PassThrough("master-ls-for-intake-job")
        
        val process: ProcessBuilder = Process(commandToRun, workDir.toFile)
        
        info(s"Ran LS, exit code ${process.!(processLogger)}")
      }
    }
  }
}

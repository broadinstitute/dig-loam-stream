package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.util.Paths
import loamstream.TestHelpers
import loamstream.util.Files
import loamstream.util.TimeUtils
import scala.util.control.NonFatal
import loamstream.loam.LoamSyntax
import loamstream.util.Loggable
import loamstream.compiler.LoamEngine
import loamstream.model.execute.RxExecuter

/**
 * @author clint
 * Apr 6, 2020
 */
final class ProcessRealDataTest extends FunSuite with Loggable {
  test("Real input produces output that matches the existing/old Perl scripts") {
    import IntakeSyntax._
    import TestHelpers._
  
    object ColumnNames {
      val VAR_ID = "VAR_ID".asColumnName
      val CHR = "CHR".asColumnName
      val POS = "POS".asColumnName
      val BP = "BP".asColumnName
      val ALLELE0 = "ALLELE0".asColumnName
      val ALLELE1 = "ALLELE1".asColumnName
      val EAF_PH = "EAF_PH".asColumnName
      val MAF_PH = "MAF_PH".asColumnName
      val A1FREQ = "A1FREQ".asColumnName
      val BETA = "BETA".asColumnName
      val SE = "SE".asColumnName
      val P_VALUE = "P_VALUE".asColumnName
      val P_BOLT_LMM = "P_BOLT_LMM".asColumnName
    }
    
    import ColumnNames._
    import AggregatorColumnDefs._
  
    val toAggregatorRows: AggregatorRowExpr = {
      AggregatorRowExpr(
        markerDef = marker(
                      chromColumn = CHR, 
                      posColumn = BP, 
                      refColumn = ALLELE0, 
                      altColumn = ALLELE1, 
                      destColumn = VAR_ID),
        pvalueDef = NamedColumnDef(P_VALUE, P_BOLT_LMM.asDouble, None),
        stderrDef = Some(NamedColumnDef(SE, SE.asDouble)),
        betaDef = Some(beta(BETA, destColumn = BETA)),
        eafDef = Some(eaf(A1FREQ, destColumn = EAF_PH)),
        mafDef = Some(NamedColumnDef(MAF_PH, A1FREQ.asDouble.complementIf(_ > 0.5))))
  }
        
    val flipDetector: FlipDetector = new FlipDetector.Default(
        referenceDir = path("src/test/resources/intake/reference-first-1M-of-chrom1"),
        isVarDataType = true,
        pathTo26kMap = path("src/test/resources/intake/26k_id.map.first100"))
  
    val source = Source.fromFile(
        path("src/test/resources/intake/real-input-data.tsv"), 
        Source.Formats.tabDelimitedWithHeader.withDelimiter(' '))
    
    TestHelpers.withWorkDir(this.getClass.getSimpleName) { workDir =>
      val (actualDataPath, graph) = TestHelpers.withScriptContext { implicit scriptContext =>
        import LoamSyntax._
        
        val destPath = workDir.resolve("processed.tsv")
        
        val dest = store(destPath)
        
        val sourceStore = store(path("src/test/resources/intake/real-input-data.tsv"))
        
        val filterLog: Store = store(path(s"${dest.path.toString}.filtered-rows"))
        
        val summaryStatsFile: Store = store(path(s"${dest.path.toString}.summary"))
        
        produceCsv(dest).
          from(source).
          using(flipDetector).
          via(toAggregatorRows).
          filter(DataRowFilters.validEaf(filterLog, append = true)).
          filter(DataRowFilters.validMaf(filterLog, append = true)).
          filter(DataRowFilters.validPValue(filterLog, append = true)).
          map(DataRowTransforms.clampPValues(filterLog, append = true)).
          writeSummaryStatsTo(summaryStatsFile).
          write(forceLocal = true).
          tag(s"process-real-data").
          in(sourceStore)
          
        (destPath, scriptContext.projectContext.graph)
      }
      
      val executable = LoamEngine.toExecutable(graph)
        
      val results = RxExecuter.default.execute(executable)
    
      val expected = Source.fromFile(path("src/test/resources/intake/real-output-data.tsv"))
      
      val actual = Source.fromFile(actualDataPath)
      
      val expectations: Iterable[(String, (String, String) => Unit)] = {
        def asDouble(fieldName: String): (String, (String, String) => Unit) = { 
          fieldName -> { (lhs, rhs) => 
            compareWithEpsilon(fieldName, lhs.toDouble, rhs.toDouble)
          }
        }
        
        def asString(fieldName: String): (String, (String, String) => Unit) = { 
          fieldName ->  { (lhs, rhs) => 
            assert(lhs == rhs, s"Field '$fieldName' didn't match")
          }
        }
        
        Seq(
          asString(VAR_ID.name),
          asDouble(P_VALUE.name),
          asDouble(SE.name),
          asDouble(BETA.name),
          asDouble(EAF_PH.name),
          asDouble(MAF_PH.name))
      }
        
      val actualRecords = actual.records.toList
      val expectedRecords = expected.records.toList
      
      actualRecords.zip(expectedRecords).foreach { case (lhs, rhs) => assertSame(lhs, rhs, expectations) }
      
      assert(actualRecords.size === expectedRecords.size)
      
      //TODO: XXX
      println(s"SUMMARY STATS: '${loamstream.util.Files.readFrom(path(s"${actualDataPath}.summary"))}'")
    }
  }
  
  private def assertSame(
      lhs: CsvRow, 
      rhs: CsvRow, 
      expectations: Iterable[(String, (String, String) => Unit)]): Unit = {
    
    try {
      expectations.foreach { case (fieldName, doAssertion) => 
        doAssertion(lhs.getFieldByName(fieldName), rhs.getFieldByName(fieldName))
      }
    } catch {
      case NonFatal(e) => {
        val msg = {
          def mkString(row: CsvRow): String = row.values.mkString(" ")
          
          s"Rows didn't match: \nactual: '${mkString(lhs)}'\nexpected: '${mkString(rhs)}', '$lhs' != '$rhs'"
        }
        
        throw new Exception(msg, e)
      }
    }
    
    assert(lhs.size === expectations.size)
    assert(rhs.size === expectations.size)
    
    assert(lhs.size === rhs.size)
  }
  
  private val epsilon = 1e-8d
  
  private implicit val doubleEq = org.scalactic.TolerantNumerics.tolerantDoubleEquality(epsilon)
  
  private def compareWithEpsilon(fieldName: String, a: Double, b: Double): Unit = {
    //import ProcessRealDataTest.Implicits._
    
    assert(a === b, s"Field '$fieldName' didn't match")
  }
}


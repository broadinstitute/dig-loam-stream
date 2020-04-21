package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.util.Paths
import loamstream.TestHelpers
import loamstream.util.Files
import loamstream.util.TimeUtils
import scala.util.control.NonFatal

/**
 * @author clint
 * Apr 6, 2020
 */
final class ProcessRealDataTest extends FunSuite {
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
  
    val a1Freq = A1FREQ.asDouble
    val beta = BETA.asDouble
    
    val rowDef = UnsourcedRowDef(
      varIdDef = ColumnDef(
        VAR_ID, 
        strexpr"${CHR}_${BP}_${ALLELE0}_${ALLELE1}", 
        strexpr"${CHR}_${BP}_${ALLELE1}_${ALLELE0}"),
      otherColumns = Seq(
        ColumnDef(EAF_PH, a1Freq, 1.0 - a1Freq),
        ColumnDef(MAF_PH, a1Freq.complementIf(_ > 0.5)),
        ColumnDef(BETA, beta, -beta),
        ColumnDef(SE, SE, SE),
        ColumnDef(P_VALUE, P_BOLT_LMM, P_BOLT_LMM)))
    
    val flipDetector: FlipDetector = new FlipDetector.Default(
        referenceDir = path("src/test/resources/intake/reference-first-1M-of-chrom1"),
        isVarDataType = true,
        pathTo26kMap = path("src/test/resources/intake/26k_id.map.first100"))
  
    val input = CsvSource.fromFile(
        path("src/test/resources/intake/real-input-data.tsv"), 
        CsvSource.Defaults.Formats.tabDelimitedWithHeaderCsvFormat.withDelimiter(' '))
    
    val (headerRow: HeaderRow, resultIterator: Iterator[DataRow]) = process(flipDetector)(rowDef.from(input))
        
    val expected = CsvSource.fromFile(path("src/test/resources/intake/real-output-data.tsv"))
    
    TestHelpers.withWorkDir(this.getClass.getSimpleName) { workDir =>
      
      val actualDataPath = workDir.resolve("processed.tsv")
      
      val renderer: Renderer = Renderer.CommonsCsv(CsvSource.Defaults.Formats.tabDelimitedWithHeaderCsvFormat)
      
      Files.writeLinesTo(actualDataPath)(Iterator(renderer.render(headerRow)) ++ resultIterator.map(renderer.render))
      
      val actual = CsvSource.fromFile(actualDataPath)
      
      val expectations: Iterable[(String, (String, String) => Unit)] = {
        val asDouble: (String, String) => Unit = { (lhs, rhs) => compareWithEpsilon(lhs.toDouble, rhs.toDouble) }
        val asString: (String, String) => Unit = _ == _
        
        Seq(
          VAR_ID.name -> asString,
          EAF_PH.name -> asDouble,
          MAF_PH.name -> asDouble,
          BETA.name -> asDouble,
          SE.name -> asDouble,
          P_VALUE.name -> asDouble)
      }
        
      actual.records.zip(expected.records).foreach { case (lhs, rhs) => assertSame(lhs, rhs, expectations) }
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
          
          s"Rows didn't match: \nactual: '${mkString(lhs)}'\nexpected: '${mkString(rhs)}'"
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
  
  private def compareWithEpsilon(a: Double, b: Double): Unit = {
    //import ProcessRealDataTest.Implicits._
    
    assert(a === b)
  }
}


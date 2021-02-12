package loamstream.loam.intake.dga

import org.scalatest.FunSuite
import loamstream.loam.intake.ColumnExpr
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.Helpers

/**
 * @author clint
 * Feb 11, 2021
 */
final class BedRowExprTest extends FunSuite {
  test("literal columns") {
    def doTest[A](column: BedRowExpr.Columns => ColumnExpr[A], expected: Annotation => A): Unit = {
      val row: DataRow = EmptyDataRow

      assert(column(columns)(row) === expected(annotation))
    }

    doTest(_.dataset, _.annotationId)
    doTest(_.biosampleId, _.biosampleId)
    doTest(_.biosampleType, _.biosampleType)
    doTest(_.biosample, _.biosample)
    doTest(_.tissueId, _.tissueId)
    doTest(_.tissue, _.tissue)
    doTest(_.annotation, _.annotationType.replaceAll("\\s+", "_"))
    doTest(_.category, _.category)
    doTest(_.method, _.method)
    doTest(_.source, _.source)
    doTest(_.assay, _.assay)
    doTest(_.collection, _.collection)
  }

  def doNaValuesTest[A](
      columns: BedRowExpr.Columns = columns)(
      column: BedRowExpr.Columns => ColumnExpr[Option[A]], 
      columnNames: String*): Unit = {
    
    val rows = for {
      v <- pandasDefaultNaValues.toSeq
      columnName <- columnNames
    } yield Helpers.csvRow("foo" -> "bar", columnName -> v)

    rows.foreach { r =>
      assert(column(columns)(r) === None)
    }
  }

  //See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
  private val pandasDefaultNaValues: Set[String] = Set(
    "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
    "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null")

  test("chromosome column") {
    val columnNames = Seq("chromosome", "chr", "chrom")
    
    doNaValuesTest()(_.chromosome, columnNames: _*)
    
    for {
      columnName <- columnNames
    } {
      def doChromTest(input: String, expected: String): Unit = {
        val row = Helpers.csvRow("foo" -> "bar", columnName -> input)
        
        assert(columns.chromosome(row) === Some(expected))
      }
      
      doChromTest("1", "1")
      doChromTest("chr1", "1")
      doChromTest("ChR17", "17")
      doChromTest("x", "X")
      doChromTest("23", "X")
      doChromTest("chr23", "X")
      doChromTest("Y", "Y")
      doChromTest("24", "Y")
      doChromTest("chr24", "Y")
      doChromTest("Xy", "XY")
      doChromTest("25", "XY")
      doChromTest("CHR25", "XY")
      doChromTest("26", "MT")
      doChromTest("cHr26", "MT")
      doChromTest("m", "MT")
      doChromTest("M", "MT")
    }
  }

  test("start column") {
    val columnNames = Seq("start", "chromStart")

    doNaValuesTest()(_.start, columnNames: _*)

    for {
      columnName <- columnNames
    } {
      val row = Helpers.csvRow("foo" -> "bar", columnName -> "42")

      assert(columns.start(row) === Some(42L))
    }
  }

  test("end column") {
    val columnNames = Seq("end", "chromEnd")

    doNaValuesTest()(_.end, columnNames: _*)

    for {
      columnName <- columnNames
    } {
      val row = Helpers.csvRow("foo" -> "bar", columnName -> "42")

      assert(columns.end(row) === Some(42L))
    }
  }

  test("state column") {
    val columnNames = Seq("state", "name")

    doNaValuesTest()(_.state, columnNames: _*)

    for {
      columnName <- columnNames
    } {
      val row = Helpers.csvRow("foo" -> "bar", columnName -> "blah")

      assert(columns.state(row) === Some("blah"))

      val newColumns = columns.copy(ann = annotation.copy(annotationType = "accessible_chromatin"))

      assert(newColumns.state(row) === Some("accessible_chromatin"))
    }
  }

  private def doTargetGeneColumnTest[A](
      columnName: String, 
      expected: A, 
      column: BedRowExpr.Columns => ColumnExpr[Option[A]]): Unit = {
    
    val row = Helpers.csvRow("foo" -> "bar", columnName -> expected.toString)
    
    assert(columns.ann.annotationType !== "target_gene_prediction")
    
    assert(column(columns)(row) === None)
    
    val newColumns = BedRowExpr.Columns(annotation.copy(annotationType = "target_gene_prediction"))
    
    doNaValuesTest(newColumns)(column, columnName)
    
    assert(column(newColumns)(row) === Some(expected))
  }

  test("targetGene column") {
    doTargetGeneColumnTest("target_gene", "blerg", _.targetGene)
  }

  test("targetGeneStart column") {
    doTargetGeneColumnTest("target_gene_start", 42L, _.targetGeneStart)
  }

  test("targetGeneEnd column") {
    doTargetGeneColumnTest("target_gene_end", 99L, _.targetGeneEnd)
  }

  private object EmptyDataRow extends DataRow {
    override def isSkipped: Boolean = false

    override def skip: DataRow = ???

    override def headers: Seq[String] = Nil

    override def hasField(name: String): Boolean = false

    override def getFieldByName(name: String): String = ???

    override def getFieldByIndex(i: Int): String = ???

    override def recordNumber: Long = ???

    override def size: Int = 0
  }

  private val assembly: String = "asdasdasfa"
  private val annotationType: String = "asfkahskjgs"
  private val annotationId: String = "ASKJhkjasf"
  private val category: Option[String] = Some("fhdolhujd")
  private val tissueId: Option[String] = Some("sdgpl89dg")
  private val tissue: Option[String] = Some("v9j0 8sdf")
  private val source: Option[String] = Some("c8bvfxf")
  private val assay: Option[String] = Some("sdog89dfsdfgas")
  private val collection: Option[String] = Some("vcb8ouxd.,mf")
  private val biosampleId: String = "ldsfgkn b"
  private val biosampleType: String = "dlskjflsdfa1"
  private val biosample: Option[String] = Some(",mvn,vnmv,b")
  private val method: Option[String] = Some("lmvlfdtttt")
  private val portalUsage: String = "asdasdasdasdasd"

  private val annotation = Annotation(
    assembly = assembly,
    annotationType = annotationType,
    annotationId = annotationId,
    category = category,
    tissueId = tissueId,
    tissue = tissue,
    source = source,
    assay = assay,
    collection = collection,
    biosampleId = biosampleId,
    biosampleType = biosampleType,
    biosample = biosample,
    method = method,
    portalUsage = portalUsage,
    downloads = Nil)

  private val columns = BedRowExpr.Columns(annotation)

}

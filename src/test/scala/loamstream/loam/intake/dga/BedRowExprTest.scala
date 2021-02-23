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
  test("Apply - good input") {
    val chromColumnNames = Seq("chromosome", "chr", "chrom")
    val startColumnNames = Seq("start", "chromStart")
    val endColumnNames = Seq("end", "chromEnd")
    val stateColumnNames = Seq("state", "name")
    
    val annotations = Seq(
        annotation.copy(annotationType = "accessible_chromatin"),
        annotation.copy(annotationType = "target_gene_prediction"),
        annotation)
    
    for {
      chromColumn <- chromColumnNames
      startColumn <- startColumnNames
      endColumn <- endColumnNames
      stateColumn <- stateColumnNames
      ann <- annotations
    } {
      val row = Helpers.csvRow(
          "foo" -> "bar", 
          chromColumn -> "cHr14",
          startColumn -> "5432",
          "baz" -> "blerg",
          endColumn -> "123456",
          stateColumn -> "asdfghjkl",
          "zuh" -> "bip",
          "target_gene" -> "qwerty",
          "target_gene_start" -> "456",
          "target_gene_end" -> "789")
       
      val expr = BedRowExpr(ann)          
          
      val bedRow = expr(row).get
      
      assert(bedRow.dataset === ann.annotationId)
      assert(bedRow.biosampleId === ann.biosampleId)
      assert(bedRow.biosampleType === ann.biosampleType)
      assert(bedRow.biosample === ann.biosample)
      assert(bedRow.tissueId === ann.tissueId)
      assert(bedRow.tissue === ann.tissue)
      assert(bedRow.annotation === ann.annotationType)
      assert(bedRow.category === ann.category)
      assert(bedRow.method === ann.method)
      assert(bedRow.source === ann.source)
      assert(bedRow.assay === ann.assay)
      assert(bedRow.collection === ann.collection)
      assert(bedRow.chromosome === "14")
      assert(bedRow.start === 5432L)
      assert(bedRow.end === 123456L)
      
      val expectedState = if(ann.annotationType == "accessible_chromatin") "accessible_chromatin" else "asdfghjkl"
      
      assert(bedRow.state === expectedState)
        
      val (expectedTargetGene: Option[String], 
           expectedTargetGeneStart: Option[Long], 
           expectedTargetGeneEnd: Option[Long]) = {
        
        if(ann.annotationType == "target_gene_prediction") { (Some("qwerty"), Some(456L), Some(789L)) }
        else { (None, None, None) }
      }
      
      assert(bedRow.targetGene === expectedTargetGene)
      assert(bedRow.targetGeneStart === expectedTargetGeneStart)
      assert(bedRow.targetGeneEnd === expectedTargetGeneEnd)
    }
  }
  
  test("Apply - bad input") {
    val chromColumnNames = Seq("chromosome", "chr", "chrom")
    val startColumnNames = Seq("start", "chromStart")
    val endColumnNames = Seq("end", "chromEnd")
    val stateColumnNames = Seq("state", "name")

    def doTest(
        badChrom: Boolean = false, 
        badStart: Boolean = false, 
        badEnd: Boolean = false, 
        badState: Boolean = false): Unit = {
      for {
        chromColumn <- chromColumnNames
        startColumn <- startColumnNames
        endColumn <- endColumnNames
        stateColumn <- stateColumnNames
      } {
        val row = Helpers.csvRow(
            "foo" -> "bar", 
            chromColumn -> (if(badChrom) "" else "cHr14"),
            startColumn -> (if(badStart) "hello" else "5432"),
            "baz" -> "blerg",
            endColumn -> (if(badEnd) "lol" else "123456"),
            stateColumn -> (if(badState) "" else "asdfghjkl"),
            "zuh" -> "bip",
            "target_gene" -> "qwerty",
            "target_gene_start" -> "456",
            "target_gene_end" -> "789")
         
        val expr = BedRowExpr(annotation)         
        
        assert(expr(row).isFailure)
      }
    }
    
    doTest(badChrom = true)
    doTest(badStart = true)
    doTest(badEnd = true)
    doTest(badState = true)
  }
  
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
    doTest(_.annotation, _.annotationType)
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
      v <- naValues.toSeq
      columnName <- columnNames
    } yield Helpers.csvRow("foo" -> "bar", columnName -> v)

    rows.foreach { r =>
      assert(column(columns)(r) === None)
    }
  }

  //See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
  private val naValues: Set[String] = Set(
    "", ".", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
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

  test("state column - unprefixed, no harmonized states") {
    val columnNames = Seq("state", "name")

    doNaValuesTest()(_.state, columnNames: _*)

    assert(annotation.harmonizedStates.isEmpty)
    
    for {
      columnName <- columnNames
    } {
      val row = Helpers.csvRow("foo" -> "bar", columnName -> "blah")

      assert(columns.state(row) === Some("blah"))

      {
        val newColumns = columns.copy(ann = annotation.copy(annotationType = "accessible_chromatin"))
        
        assert(newColumns.state(row) === Some("accessible_chromatin"))
      }

      {
        val newColumns = {
          columns.copy(ann = annotation.copy(harmonizedStates = Some(Map("x" -> "y", "blah" -> "blerg"))))
        }
        
        assert(newColumns.state(row) === Some("blerg"))
      }
    }
  }
  
  test("state column - prefixed") {
    val columnNames = Seq("state", "name")

    doNaValuesTest()(_.state, columnNames: _*)

    assert(annotation.harmonizedStates.isEmpty)
    
    for {
      columnName <- columnNames
    } {
      val row = Helpers.csvRow("foo" -> "bar", columnName -> "54321_blah")

      assert(columns.state(row) === Some("blah"))

      {
        val newColumns = columns.copy(ann = annotation.copy(annotationType = "accessible_chromatin"))
        
        assert(newColumns.state(row) === Some("accessible_chromatin"))
      }

      {
        val newColumns = {
          columns.copy(ann = annotation.copy(harmonizedStates = Some(Map("x" -> "y", "blah" -> "blerg"))))
        }
        
        assert(newColumns.state(row) === Some("blerg"))
      }
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
  private val assay: Option[Seq[String]] = Some(Seq("sdog89dfsdfgas", "sadxccvbvbmbnm"))
  private val collections: Option[Seq[String]] = Some(Seq("vcb8ouxd.","mf"))
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
    collection = collections,
    biosampleId = biosampleId,
    biosampleType = biosampleType,
    biosample = biosample,
    method = method,
    portalUsage = portalUsage,
    harmonizedStates = None,
    downloads = Nil)

  private val columns = BedRowExpr.Columns(annotation)

}

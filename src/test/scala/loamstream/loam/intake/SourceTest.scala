package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.util.Files
import java.io.StringReader
import loamstream.loam.intake.metrics.MetricTest.MockFlipDetector
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.flip.Disposition

/**
 * @author clint
 * Feb 27, 2020
 */
final class SourceTest extends FunSuite {
  
  private val rowData: Seq[Seq[String]] = {
    Seq(
      Seq("FOO", "BAR", "BAZ"),
      Seq("1",   "2",   "3"),
      Seq("9",   "8",   "7"),
      Seq("42",  "99",  "123"))
  }
  
  private val csvRows: Seq[CsvRow] = Helpers.csvRows(rowData.head, rowData.tail: _*)
  
  private def rowDataToString(data: Seq[Seq[String]]): String = {
    data.map(_.mkString("\t")).mkString(System.lineSeparator)
  }
  
  private val rowDataAsString = rowDataToString(rowData)
    
  private def doCsvSourceRecordsTest(source: Source[CsvRow], expectedRows: Seq[CsvRow] = csvRows): Unit = {
    doCsvSourceRecordsTest(source.records.toIndexedSeq, expectedRows)
  }
  
  private def doCsvSourceRecordsTest(actualRows: Seq[CsvRow], expectedRows: Seq[CsvRow]): Unit = {
    
    actualRows.zip(expectedRows).foreach { case (actualRow, expectedRow) =>
      assert(actualRow.getFieldByName("FOO") === expectedRow.getFieldByName("FOO"))
      assert(actualRow.getFieldByName("BAR") === expectedRow.getFieldByName("BAR"))
      assert(actualRow.getFieldByName("BAZ") === expectedRow.getFieldByName("BAZ"))
    }
    
    assert(actualRows.size === expectedRows.size)
  }
  
  
  
  test("fromString") {
    doCsvSourceRecordsTest(Source.fromString(rowDataAsString))
  }
  
  test("fromFile") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("data.csv")
      
      Files.writeTo(file)(rowDataAsString) 
      
      doCsvSourceRecordsTest(Source.fromFile(file))
    }
  }
  
  test("fromReader") {
    doCsvSourceRecordsTest(Source.fromReader(new StringReader(rowDataAsString)))
  }
  
  test("fromCommandLine") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      val file = workDir.resolve("data.csv")
      
      Files.writeTo(file)(rowDataAsString) 
      
      doCsvSourceRecordsTest(Source.fromCommandLine(s"cat $file", workDir = workDir))
    }
  }
  
  test("take") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val source = Source.fromReader(reader).take(2)
    
    val truncatedRowData = rowData.head +: rowData.tail.take(2)
    
    doCsvSourceRecordsTest(source, Helpers.csvRows(truncatedRowData.head, truncatedRowData.tail: _*))
  }
  
  test("filter") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val bar = ColumnName("BAR")
    
    val source = Source.fromReader(reader).filter(bar.asInt > 10)
    
    val expectedRows = Helpers.csvRows(
        Seq("FOO", "BAR", "BAZ"),
        Seq("42",  "99",  "123"))
        
    doCsvSourceRecordsTest(source, expectedRows)
  }
  
  test("filterNot") {
    val reader = new StringReader(rowDataToString(rowData))
    
    val bar = ColumnName("BAR")
    
    val source = Source.fromReader(reader).filterNot(bar.asInt > 10)
    
    val expectedRows = Helpers.csvRows(
        Seq("FOO", "BAR", "BAZ"),
        Seq("1",   "2",   "3"),
        Seq("9",   "8",   "7"))
        
    doCsvSourceRecordsTest(source, expectedRows)
  }
  
  test("tagFlips - no flips, no complements") {
    val rowData: Seq[Seq[String]] = Seq(
        /* 0 */ Seq("FOO", "BAR", "BAZ"),
        /* 1 */ Seq("1_1_X_Y",   "2",   "3"),
        /* 2 */ Seq("1_2_A_B",   "8",   "7"),
        /* 3 */ Seq("2_1_U_V",   "9",   "17"),
        /* 4 */ Seq("2_2_Q_R",   "10",   "27"))
    
    val reader = new StringReader(rowDataToString(rowData))
    
    val foo = ColumnName("FOO")
    val bar = ColumnName("BAR")
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, foo.map(Variant.from))
    
    val flipDetector: FlipDetector = SourceTest.MockFlipDetector(Set.empty, Set.empty)
    
    val source = Source.fromReader(reader).tagFlips(markerDef, flipDetector)
    
    val untaggedExpectedRows = Helpers.csvRows(rowData.head, rowData.tail: _*)
        
    val expectedRows = untaggedExpectedRows.map { r =>
      val marker = Variant.from(r.getFieldByName("FOO"))
      
      CsvRow.TaggedCsvRow(r, marker, marker, Disposition.NotFlippedSameStrand)
    }
        
    val actualRows = source.records.toIndexedSeq
    
    expectedRows.forall(r => r.marker === r.originalMarker)
    expectedRows.forall(_.disposition === Disposition.NotFlippedSameStrand)
    
    val rawActualRows = actualRows.map(_.asInstanceOf[CsvRow.TaggedCsvRow].delegate)
    
    doCsvSourceRecordsTest(rawActualRows, expectedRows)
  }
  
  test("tagFlips - some flips, no complements") {
    val m0 = Variant.from("1_1_X_Y")
    val m1 = Variant.from("1_2_A_B")
    val m2 = Variant.from("2_1_U_V")
    val m3 = Variant.from("2_2_Q_R")
    
    val rowData: Seq[Seq[String]] = Seq(
      /* 0 */ Seq("FOO", "BAR", "BAZ"),
      /* 1 */ Seq(m0.underscoreDelimited,   "2",   "3"),
      /* 2 */ Seq(m1.underscoreDelimited,   "8",   "7"),
      /* 3 */ Seq(m2.underscoreDelimited,   "9",   "17"),
      /* 4 */ Seq(m3.underscoreDelimited,   "10",   "27"))
    
    val reader = new StringReader(rowDataToString(rowData))
    
    val foo = ColumnName("FOO")
    val bar = ColumnName("BAR")
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, foo.map(Variant.from))
    
    val flippedVariants = Set(m0, m3)
    
    val flipDetector: FlipDetector = SourceTest.MockFlipDetector(flippedVariants = flippedVariants, Set.empty)
    
    val source = Source.fromReader(reader).tagFlips(markerDef, flipDetector)
    
    val untaggedExpectedRows = Helpers.csvRows(rowData.head, rowData.tail: _*)
      
    def marker(r: CsvRow) = Variant.from(r.getFieldByName("FOO"))
    
    def makeTaggedRow(i: Int, disp: Disposition) = {
      val mrkr = marker(untaggedExpectedRows(1))
      
      CsvRow.TaggedCsvRow(untaggedExpectedRows(i), mrkr, mrkr, disp)
    }
    
    val expectedRows = Seq(
      makeTaggedRow(0, Disposition.FlippedSameStrand),
      makeTaggedRow(1, Disposition.NotFlippedSameStrand),
      makeTaggedRow(2, Disposition.NotFlippedSameStrand),
      makeTaggedRow(3, Disposition.FlippedSameStrand))
        
    val actualRows = source.records.toIndexedSeq
    
    def shouldBeFlipped(r: CsvRow.TaggedCsvRow): Boolean = flippedVariants.contains(r.originalMarker)
    
    val shouldBeFlippedRows = expectedRows.filter(shouldBeFlipped)
    val shouldNOTBeFlippedRows = expectedRows.filterNot(shouldBeFlipped)
    
    shouldBeFlippedRows.forall(r => r.marker === r.originalMarker.flip)
    shouldNOTBeFlippedRows.forall(r => r.marker === r.originalMarker)
    
    shouldBeFlippedRows.forall(_.disposition === Disposition.FlippedSameStrand)
    shouldNOTBeFlippedRows.forall(_.disposition === Disposition.NotFlippedSameStrand)
    
    val rawActualRows = actualRows.map(_.asInstanceOf[CsvRow.TaggedCsvRow].delegate)
    
    doCsvSourceRecordsTest(rawActualRows, expectedRows)
  }
  
  test("tagFlips - some flips, some complements") {
    fail("TODO")
  }
  
  test("tagFlips - no flips, some complements") {
    fail("TODO")
  }
}

object SourceTest {
  private final case class MockFlipDetector(
      flippedVariants: Set[Variant], 
      complementedVariants: Set[Variant]) extends FlipDetector {
    
    override def isFlipped(variantId: Variant): Disposition = {
      val flipped = flippedVariants.contains(variantId)
      val complemented = complementedVariants.contains(variantId)
      
      if(flipped && complemented) { Disposition.FlippedComplementStrand }
      else if(flipped) { Disposition.FlippedSameStrand }
      else if(complemented) { Disposition.NotFlippedComplementStrand }
      else { Disposition.NotFlippedSameStrand }
    }
  }
}

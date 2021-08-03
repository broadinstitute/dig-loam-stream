package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.TestHelpers.path
import loamstream.util.Files
import java.io.StringReader
import loamstream.loam.intake.metrics.MetricTest.MockFlipDetector
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.flip.Disposition

import scala.collection.compat._

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
  
  private val csvRows: Seq[DataRow] = Helpers.csvRows(rowData.head, rowData.tail: _*)
  
  private def rowDataToString(data: Seq[Seq[String]]): String = {
    data.map(_.mkString("\t")).mkString(System.lineSeparator)
  }
  
  private val rowDataAsString = rowDataToString(rowData)
    
  private def doCsvSourceRecordsTest(source: Source[DataRow], expectedRows: Seq[DataRow] = csvRows): Unit = {
    doCsvSourceRecordsTest(source.records.toIndexedSeq, expectedRows)
  }
  
  private def doCsvSourceRecordsTest(actualRows: Seq[DataRow], expectedRows: Seq[DataRow]): Unit = {
    actualRows.zip(expectedRows).foreach { case (actualRow, expectedRow) =>
      assert(actualRow.getFieldByName("FOO") === expectedRow.getFieldByName("FOO"))
      assert(actualRow.getFieldByName("BAR") === expectedRow.getFieldByName("BAR"))
      assert(actualRow.getFieldByName("BAZ") === expectedRow.getFieldByName("BAZ"))
    }
    
    assert(actualRows.size === expectedRows.size)
  }
  
  test("flatMap") {
    val is = Source.fromIterable(Seq(1,2,3)).flatMap { i => Source.fromIterable(Seq.fill(i)(i * 2)) }
    
    val expected = Seq(2, 4, 4, 6, 6, 6)
    
    assert(is.records.to(List) === expected)
  }
  
  test("Reading zipped input as unzipped should fail") {
    intercept[Exception] {
      Source.fromFile(
          path("src/test/resources/intake/real-input-data.tsv.gz"), 
          Source.Formats.spaceDelimitedWithHeader)
    }
  }
  
  test("Reading unzipped input as zipped should fail") {
    intercept[Exception] {
      Source.fromGzippedFile(
          path("src/test/resources/intake/real-input-data.tsv"), 
          Source.Formats.spaceDelimitedWithHeader)
    }
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
  
  private val m0 = Variant.from("1_1_A_T")
  private val m1 = Variant.from("1_2_G_C")
  private val m2 = Variant.from("2_1_T_C")
  private val m3 = Variant.from("2_2_C_G")
  
  import loamstream.util.LogContext.Implicits.Noop

  test("tagFlips - no flips, no complements") {
    val rowData: Seq[Seq[String]] = Seq(
        /* 0 */ Seq("FOO", "BAR", "BAZ"),
        /* 1 */ Seq(m0.underscoreDelimited, "2",  "3"),
        /* 2 */ Seq(m1.underscoreDelimited, "8",  "7"),
        /* 3 */ Seq(m2.underscoreDelimited, "9",  "17"),
        /* 4 */ Seq(m3.underscoreDelimited, "10", "27"))
    
    val reader = new StringReader(rowDataToString(rowData))
    
    val foo = ColumnName("FOO")
    val bar = ColumnName("BAR")
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, foo.map(Variant.from))
    
    val flipDetector: FlipDetector = SourceTest.MockFlipDetector(Set.empty, Set.empty)
    
    val source = Source.fromReader(reader).tagFlips(markerDef, flipDetector)
    
    val untaggedExpectedRows = Helpers.csvRows(rowData.head, rowData.tail: _*)
        
    val expectedRows = untaggedExpectedRows.map { r =>
      val marker = Variant.from(r.getFieldByName("FOO"))
      
      VariantRow.Analyzed.Tagged(r, marker, marker, Disposition.NotFlippedSameStrand)
    }
        
    val actualRows = source.records.toIndexedSeq
    
    expectedRows.forall(r => r.marker === r.originalMarker)
    expectedRows.forall(_.disposition === Disposition.NotFlippedSameStrand)
    
    val rawActualRows = actualRows.map(_.asInstanceOf[VariantRow.Analyzed.Tagged].derivedFrom)
    
    doCsvSourceRecordsTest(rawActualRows, expectedRows.map(_.derivedFrom))
  }
  
  test("tagFlips - some flips, no complements") {
    val rowData: Seq[Seq[String]] = Seq(
      /* 0 */ Seq("FOO", "BAR", "BAZ"),
      /* 1 */ Seq(m0.underscoreDelimited, "2",  "3"),
      /* 2 */ Seq(m1.underscoreDelimited, "8",  "7"),
      /* 3 */ Seq(m2.underscoreDelimited, "9",  "17"),
      /* 4 */ Seq(m3.underscoreDelimited, "10", "27"))
    
    val reader = new StringReader(rowDataToString(rowData))
    
    val foo = ColumnName("FOO")
    val bar = ColumnName("BAR")
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, foo.map(Variant.from))
    
    val flippedVariants = Set(m0, m3)
    
    val flipDetector: FlipDetector = SourceTest.MockFlipDetector(flippedVariants = flippedVariants, Set.empty)
    
    val source = Source.fromReader(reader).tagFlips(markerDef, flipDetector)
    
    val untaggedExpectedRows = Helpers.csvRows(rowData.head, rowData.tail: _*)
      
    def marker(r: DataRow) = Variant.from(r.getFieldByName("FOO"))
    
    def makeTaggedRow(i: Int, disp: Disposition) = {
      val mrkr = marker(untaggedExpectedRows(1))
      
      VariantRow.Analyzed.Tagged(untaggedExpectedRows(i), mrkr, mrkr, disp)
    }
    
    val expectedRows = Seq(
      makeTaggedRow(0, Disposition.FlippedSameStrand),
      makeTaggedRow(1, Disposition.NotFlippedSameStrand),
      makeTaggedRow(2, Disposition.NotFlippedSameStrand),
      makeTaggedRow(3, Disposition.FlippedSameStrand))
        
    val actualRows = source.records.toIndexedSeq
    
    def shouldBeFlipped(r: VariantRow.Analyzed.Tagged): Boolean = flippedVariants.contains(r.originalMarker)
    
    val shouldBeFlippedRows = expectedRows.filter(shouldBeFlipped)
    val shouldNOTBeFlippedRows = expectedRows.filterNot(shouldBeFlipped)
    
    shouldBeFlippedRows.forall(r => r.marker === r.originalMarker.flip)
    shouldNOTBeFlippedRows.forall(r => r.marker === r.originalMarker)
    
    shouldBeFlippedRows.forall(_.disposition === Disposition.FlippedSameStrand)
    shouldNOTBeFlippedRows.forall(_.disposition === Disposition.NotFlippedSameStrand)
    
    val rawActualRows = actualRows.map(_.asInstanceOf[VariantRow.Analyzed.Tagged].derivedFrom)
    
    doCsvSourceRecordsTest(rawActualRows, expectedRows.map(_.derivedFrom))
  }
  
  test("tagFlips - some flips, some complements") {
    val rowData: Seq[Seq[String]] = Seq(
      /* 0 */ Seq("FOO", "BAR", "BAZ"),
      /* 1 */ Seq(m0.underscoreDelimited, "2",  "3"),
      /* 2 */ Seq(m1.underscoreDelimited, "8",  "7"),
      /* 3 */ Seq(m2.underscoreDelimited, "9",  "17"),
      /* 4 */ Seq(m3.underscoreDelimited, "10", "27"))
    
    val reader = new StringReader(rowDataToString(rowData))
    
    val foo = ColumnName("FOO")
    val bar = ColumnName("BAR")
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, foo.map(Variant.from))
    
    val complementedVariants = Set(m0, m3)
    val flippedVariants = Set(m2)
    
    val flipDetector: FlipDetector = {
      SourceTest.MockFlipDetector(flippedVariants = flippedVariants, complementedVariants = complementedVariants)
    }
    
    val source = Source.fromReader(reader).tagFlips(markerDef, flipDetector)
    
    val untaggedExpectedRows = Helpers.csvRows(rowData.head, rowData.tail: _*)
      
    def marker(r: DataRow) = Variant.from(r.getFieldByName("FOO"))
    
    def makeTaggedRow(i: Int, disp: Disposition) = {
      val mrkr = marker(untaggedExpectedRows(1))
      
      VariantRow.Analyzed.Tagged(untaggedExpectedRows(i), mrkr, mrkr, disp)
    }
    
    val expectedRows = Seq(
      makeTaggedRow(0, Disposition.NotFlippedComplementStrand),
      makeTaggedRow(1, Disposition.NotFlippedSameStrand),
      makeTaggedRow(2, Disposition.FlippedSameStrand),
      makeTaggedRow(3, Disposition.NotFlippedComplementStrand))
        
    val actualRows = source.records.toIndexedSeq
    
    def shouldBeComplemented(r: VariantRow.Analyzed.Tagged): Boolean = complementedVariants.contains(r.originalMarker) 
    def shouldBeFlipped(r: VariantRow.Analyzed.Tagged): Boolean = flippedVariants.contains(r.originalMarker)
    def shouldBeUntouched(r: VariantRow.Analyzed.Tagged): Boolean = !shouldBeComplemented(r) && !shouldBeFlipped(r)
    
    val shouldBeComplementedRows = expectedRows.filter(shouldBeComplemented)
    val shouldBeFlippedRows = expectedRows.filter(shouldBeFlipped)
    val shouldBeUntouchedRows = expectedRows.filter(shouldBeUntouched)
    
    shouldBeComplementedRows.forall(r => r.marker === r.originalMarker.complement)
    shouldBeFlippedRows.forall(r => r.marker === r.originalMarker.flip)
    shouldBeUntouchedRows.forall(r => r.marker === r.originalMarker)
    
    shouldBeComplementedRows.forall(_.disposition === Disposition.NotFlippedComplementStrand)
    shouldBeFlippedRows.forall(_.disposition === Disposition.FlippedSameStrand)
    shouldBeUntouchedRows.forall(_.disposition === Disposition.NotFlippedSameStrand)
    
    val rawActualRows = actualRows.map(_.asInstanceOf[VariantRow.Analyzed.Tagged].derivedFrom)
    
    doCsvSourceRecordsTest(rawActualRows, expectedRows.map(_.derivedFrom))
  }
  
  test("tagFlips - no flips, some complements") {
    val rowData: Seq[Seq[String]] = Seq(
      /* 0 */ Seq("FOO", "BAR", "BAZ"),
      /* 1 */ Seq(m0.underscoreDelimited, "2",  "3"),
      /* 2 */ Seq(m1.underscoreDelimited, "8",  "7"),
      /* 3 */ Seq(m2.underscoreDelimited, "9",  "17"),
      /* 4 */ Seq(m3.underscoreDelimited, "10", "27"))
    
    val reader = new StringReader(rowDataToString(rowData))
    
    val foo = ColumnName("FOO")
    val bar = ColumnName("BAR")
    
    val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, foo.map(Variant.from))
    
    val complementedVariants = Set(m0, m3)
    
    val flipDetector: FlipDetector = SourceTest.MockFlipDetector(Set.empty, complementedVariants = complementedVariants)
    
    val source = Source.fromReader(reader).tagFlips(markerDef, flipDetector)
    
    val untaggedExpectedRows = Helpers.csvRows(rowData.head, rowData.tail: _*)
      
    def marker(r: DataRow) = Variant.from(r.getFieldByName("FOO"))
    
    def makeTaggedRow(i: Int, disp: Disposition) = {
      val mrkr = marker(untaggedExpectedRows(1))
      
      VariantRow.Analyzed.Tagged(untaggedExpectedRows(i), mrkr, mrkr, disp)
    }
    
    val expectedRows = Seq(
      makeTaggedRow(0, Disposition.NotFlippedComplementStrand),
      makeTaggedRow(1, Disposition.NotFlippedSameStrand),
      makeTaggedRow(2, Disposition.NotFlippedSameStrand),
      makeTaggedRow(3, Disposition.NotFlippedComplementStrand))
        
    val actualRows = source.records.toIndexedSeq
    
    def shouldBeComplemented(r: VariantRow.Analyzed.Tagged): Boolean = complementedVariants.contains(r.originalMarker)
    
    val shouldBeComplementedRows = expectedRows.filter(shouldBeComplemented)
    val shouldNOTBeComplementedRows = expectedRows.filterNot(shouldBeComplemented)
    
    shouldBeComplementedRows.forall(r => r.marker === r.originalMarker.complement)
    shouldNOTBeComplementedRows.forall(r => r.marker === r.originalMarker)
    
    shouldBeComplementedRows.forall(_.disposition === Disposition.NotFlippedComplementStrand)
    shouldNOTBeComplementedRows.forall(_.disposition === Disposition.NotFlippedSameStrand)
    
    val rawActualRows = actualRows.map(_.asInstanceOf[VariantRow.Analyzed.Tagged].derivedFrom)
    
    doCsvSourceRecordsTest(rawActualRows, expectedRows.map(_.derivedFrom))
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

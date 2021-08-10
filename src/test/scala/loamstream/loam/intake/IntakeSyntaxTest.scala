package loamstream.loam.intake

import org.scalatest.FunSuite
import loamstream.loam.LoamSyntax
import loamstream.TestHelpers
import java.io.Closeable
import loamstream.loam.intake.IntakeSyntaxTest.MockCloseableRowPredicate
import loamstream.util.Fold
import loamstream.loam.NativeTool
import loamstream.util.Files
import loamstream.loam.InvokesLsTool
import loamstream.drm.DrmSystem

import scala.collection.compat._
import java.io.StringReader
import loamstream.loam.intake.flip.FlipDetector
import loamstream.loam.intake.flip.Disposition

/**
 * @author clint
 * Nov 19, 2020
 */
final class IntakeSyntaxTest extends FunSuite {
  import IntakeSyntax._
  import IntakeSyntaxTest.MockCloseableRowPredicate
  import IntakeSyntaxTest.MockCloseablePredicate
  import IntakeSyntaxTest.MockCloseableTransform
  
  private val X = ColumnName("X")
  private val Y = ColumnName("Y")
  
  private val MRKR = AggregatorColumnNames.marker
  private val PValue = AggregatorColumnNames.pvalue
  
  test("asCloseable") {
    val yIsEven = Y.asInt.map(_ % 2 == 0)
    
    assert(asCloseable(yIsEven) === Nil)
    
    val closeable = MockCloseableRowPredicate(yIsEven)
    
    assert(asCloseable(closeable) === Seq(closeable))
  }
  
  private def filterSource = Helpers.sourceProducing(
    Seq(X.name, Y.name),
    Seq("a", "1"),
    Seq("b", "2"),
    Seq("c", "3"),
    Seq("d", "4"))
  
  private def toTuple(row: DataRow): (String, Boolean) = {
    s"${row.getFieldByName("X")}${row.getFieldByName("Y")}" -> row.isSkipped
  }
    
  test("ViaTarget - filter") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      
      val source = filterSource 
          
      val viaTarget = new ViaTarget(DestinationParams.To(Helpers.RowSinks.noop), source, ???)
      
      assert(viaTarget.toBeClosed === Set.empty)
      
      val yIsEven = MockCloseableRowPredicate(Y.asInt.map(_ % 2 == 0))
      
      val filtered = viaTarget.filter(yIsEven)
      
      assert(filtered.toBeClosed === Set(yIsEven))
      
      val expected = Seq(
          ("a1", true),
          ("b2", false),
          ("c3", true),
          ("d4", false))
          
      assert(filtered.rows.records.to(List).map(toTuple) === expected)
    }
  }
  
  test("ViaTarget - filter - None passed in") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      
      val source = filterSource 
          
      val viaTarget = new ViaTarget(DestinationParams.To(Helpers.RowSinks.noop), source, ???)
      
      assert(viaTarget.toBeClosed === Set.empty)
      
      val filtered = viaTarget.filter(None)
      
      assert(filtered.toBeClosed === Set.empty)
      
      val expected = Seq(
          ("a1", false),
          ("b2", false),
          ("c3", false),
          ("d4", false))
          
      assert(filtered.rows.records.to(List).map(toTuple) === expected)
    }
  }
  
  test("ViaTarget - filter - Optional predicate supplied") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      
      val source = filterSource 
          
      val viaTarget = new ViaTarget(DestinationParams.To(Helpers.RowSinks.noop), source, ???)
      
      assert(viaTarget.toBeClosed === Set.empty)
   
      val yIs3 = MockCloseableRowPredicate(Y.asInt.map(_ == 3))
      
      val filtered = viaTarget.filter(Option(yIs3))
      
      assert(filtered.toBeClosed === Set(yIs3))
      
      val expected = Seq(
          ("a1", true),
          ("b2", true),
          ("c3", false),
          ("d4", true))
          
      assert(filtered.rows.records.to(List).map(toTuple) === expected)
    }
  }
  
  private val v0 = Variant("1_2_A_T")
  private val v1 = Variant("2_3_G_C")
  private val v2 = Variant("3_4_T_A")
  private val v3 = Variant("4_5_C_G")
  
  private def mapSource = Helpers.sourceProducing(
    Seq(MRKR.name, PValue.name),
    Seq(v0.underscoreDelimited, "0.1"),
    Seq(v1.underscoreDelimited, "0.2"),
    Seq(v2.underscoreDelimited, "0.3"),
    Seq(v3.underscoreDelimited, "0.4"))
  
  private val markerDef = MarkerColumnDef(AggregatorColumnNames.marker, MRKR.map(Variant.from))
    
  private val metadata = AggregatorMetadata(
    bucketName = "some-bucket",
    topic = Option(UploadType.Variants),
    dataset = "asdasdasd",
    phenotype = "akjdslfhsdf",
    ancestry = Ancestry.AA,
    tech = TechType.ExChip,
    quantitative = None)
  
  private val expr = VariantRowExpr.PValueVariantRowExpr(
      metadata = metadata,
      markerDef = markerDef,
      pvalueDef = AggregatorColumnDefs.pvalue(PValue),
      nDef = Some(AnonColumnDef(LiteralColumnExpr(42))))
    
  test("MapFilterAndWriteTarget - withMetric") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      
      import loamstream.util.LogContext.Implicits.Noop

      val source = mapSource.map(Helpers.analyzeNoFlipsEver(markerDef)).map(expr)
          
      val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
          DestinationParams.To(Helpers.RowSinks.noop), 
          metadata,
          source, 
          Fold.foreach(_ => ()),
          Set.empty)
      
      assert(target.toBeClosed === Set.empty)
      
      val withMetric = target.withMetric(Fold.count)
      
      assert(withMetric.metric.process(source.records) === (() -> 4))
    }
  }
  
  test("MapFilterAndWriteTarget - filter - no rows already skipped") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      import loamstream.util.LogContext.Implicits.Noop

      val source = mapSource.map(Helpers.analyzeNoFlipsEver(markerDef)).map(expr)
          
      val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
          DestinationParams.To(Helpers.RowSinks.noop),
          metadata,
          source, 
          Fold.foreach(_ => ()),
          Set.empty)
      
      assert(target.toBeClosed === Set.empty)
      
      val unfilteredRows = source.records.to(List)
      
      val p = MockCloseablePredicate[PValueVariantRow](_.pvalue > 0.2)
      
      val filtered = target.filter(p)
      
      assert(filtered.toBeClosed === Set(p))
      
      val expected = unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2)
      
      assert(filtered.rows.records.to(List) === expected)
    }
  }
  
  test("MapFilterAndWriteTarget - filter - some rows already skipped") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      import loamstream.util.LogContext.Implicits.Noop

      //Source with first 2 rows already skipped
      val source = {
        val orig = mapSource.map(Helpers.analyzeNoFlipsEver(markerDef)).map(expr)
        
        val unfilteredRows = orig.records.to(List)
        
        Source.fromIterable(unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2))
      }
          
      val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
          DestinationParams.To(Helpers.RowSinks.noop), 
          metadata,
          source, 
          Fold.foreach(_ => ()),
          Set.empty)
      
      assert(target.toBeClosed === Set.empty)
      
      val unfilteredRows = source.records.to(List)
      
      val filtered = target.filter(_.pvalue > 0.3)
      
      val expected = unfilteredRows.take(3).map(_.skip) ++ unfilteredRows.drop(3)
      
      assert(filtered.rows.records.to(List) === expected)
    }
  }
  
  test("MapFilterAndWriteTarget - map") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      import loamstream.util.LogContext.Implicits.Noop
      
      //Source with first 2 rows already skipped
      val source = {
        val orig = mapSource.map(Helpers.analyzeNoFlipsEver(markerDef)).map(expr)
        
        val unfilteredRows = orig.records.to(List)
        
        Source.fromIterable(unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2))
      }
          
      val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
          DestinationParams.To(Helpers.RowSinks.noop),
          metadata,
          source, 
          Fold.foreach(_ => ()),
          Set.empty)
      
      assert(target.toBeClosed === Set.empty)
      
      val unmappedRows = source.records.to(List)
      
      val f: Transform[PValueVariantRow] = MockCloseableTransform { dr => 
        dr.copy(pvalue = dr.pvalue + 1.0)
      }
      
      val mapped = target.map(f)
      
      assert(mapped.toBeClosed === Set(f))
      
      val expected = unmappedRows.take(2) ++ unmappedRows.drop(2).map(_.transform(f))
      
      assert(mapped.rows.records.to(List) === expected)
    }
  }
  
  test("write - forceLocal=true") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      
      val outfile = workDir.resolve("out.tsv")
      
      TestHelpers.withScriptContext { implicit context =>
        import LoamSyntax._
        import loamstream.util.LogContext.Implicits.Noop
        
        //Source with first 2 rows already skipped
        val source = {
          val orig = mapSource.map(Helpers.analyzeNoFlipsEver(markerDef)).map(expr)
          
          val unfilteredRows = orig.records.to(List)
          
          Source.fromIterable(unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2))
        }
            
        val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
            DestinationParams.To(RowSink.ToFile(outfile, RowSink.Renderers.csv(Source.Formats.tabDelimited))),
            metadata,
            source, 
            Fold.foreach(_ => ()),
            Set.empty)
        
        assert(target.toBeClosed === Set.empty)
        
        val unmappedRows = source.records.to(List)
        
        val f = MockCloseableTransform[PValueVariantRow] { dr => 
          dr.copy(pvalue = dr.pvalue + 1.0)
        }
        
        val mapped = target.map(f)
        
        assert(mapped.toBeClosed === Set(f))
        
        val tool = mapped.write(forceLocal = true).asInstanceOf[NativeTool]
        
        assert(f.isClosed === false)
        
        assert(java.nio.file.Files.exists(outfile) === false)
        
        tool.body()
      
        assert(f.isClosed === true)
        
        assert(java.nio.file.Files.exists(outfile) === true)
        
        import metadata.{dataset, phenotype, ancestry}
        
        val expected = s"""
${v2.underscoreDelimited}${'\t'}${dataset}${'\t'}${phenotype}${'\t'}${ancestry.name}${'\t'}1.3${'\t'}42.0
${v3.underscoreDelimited}${'\t'}${dataset}${'\t'}${phenotype}${'\t'}${ancestry.name}${'\t'}1.4${'\t'}42.0""".trim

        //NB: trim to avoid off-by-line-ending differences we don't care about
        assert(Files.readFrom(outfile).trim === expected)
      }      
    }
  }
  
  test("write") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      
      val outfile = workDir.resolve("out.tsv")
      
      TestHelpers.withScriptContext(DrmSystem.Uger) { implicit context =>
        import LoamSyntax._
        import loamstream.util.LogContext.Implicits.Noop
        
        val source = mapSource.map(Helpers.analyzeNoFlipsEver(markerDef)).map(expr)
            
        val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
            DestinationParams.To(Helpers.RowSinks.noop),
            metadata,
            source, 
            Fold.foreach(_ => ()),
            Set.empty)
        
        assert(target.toBeClosed === Set.empty)
        
        val f = MockCloseableTransform[PValueVariantRow] { dr => 
          dr.copy(pvalue = dr.pvalue + 1.0)
        }
        
        val tool = drm {
          target.write()
        }
        
        assert(tool.isInstanceOf[InvokesLsTool])
      }      
    }
  }

  private val m0 = Variant.from("1_1_A_T")
  private val m1 = Variant.from("1_2_G_C")
  private val m2 = Variant.from("2_1_T_C")
  private val m3 = Variant.from("2_2_C_G")

  private def rowDataToString(data: Seq[Seq[String]]): String = {
    data.map(_.mkString("\t")).mkString(System.lineSeparator)
  }

  private val rowData: Seq[Seq[String]] = {
    Seq(
      Seq("FOO", "BAR", "BAZ"),
      Seq("1",   "2",   "3"),
      Seq("9",   "8",   "7"),
      Seq("42",  "99",  "123"))
  }
  
  private val csvRows: Seq[DataRow] = Helpers.csvRows(rowData.head, rowData.tail: _*)

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
    
    val flipDetector: FlipDetector = IntakeSyntaxTest.MockFlipDetector(Set.empty, Set.empty)
    
    val rows = Source.fromReader(reader)

    implicit val logCtx = loamstream.util.LogContext.Noop

    val source = IntakeSyntax.doTagFlips(rows, markerDef, flipDetector, failFast = true)
    
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
    
    val flipDetector: FlipDetector = IntakeSyntaxTest.MockFlipDetector(flippedVariants = flippedVariants, Set.empty)
    
    val rows = Source.fromReader(reader)

    implicit val logCtx = loamstream.util.LogContext.Noop

    val source = IntakeSyntax.doTagFlips(rows, markerDef, flipDetector, failFast = true)
    
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
      IntakeSyntaxTest.MockFlipDetector(flippedVariants = flippedVariants, complementedVariants = complementedVariants)
    }
    
    val rows = Source.fromReader(reader)

    implicit val logCtx = loamstream.util.LogContext.Noop

    val source = IntakeSyntax.doTagFlips(rows, markerDef, flipDetector, failFast = true)
    
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
    
    val flipDetector: FlipDetector = IntakeSyntaxTest.MockFlipDetector(Set.empty, complementedVariants = complementedVariants)
    
    /**
      * private[intake] def tagFlips(
      markerDef: MarkerColumnDef, 
      flipDetector: => FlipDetector,
      failFast: Boolean)
     (implicit logCtx: LogContext): Source[VariantRow.Analyzed] = {
      */

    val rows = Source.fromReader(reader)

    implicit val logCtx = loamstream.util.LogContext.Noop

    val source = IntakeSyntax.doTagFlips(rows, markerDef, flipDetector, failFast = true)
    
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
}

object IntakeSyntaxTest {
  final case class MockFlipDetector(
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

  final case class MockCloseableRowPredicate(p: DataRowPredicate) extends DataRowPredicate with Closeable {
    var isClosed = false
    
    override def apply(row: DataRow): Boolean = p(row)
    
    override def close(): Unit = isClosed = true
  }
  
  final case class MockCloseablePredicate[A](p: Predicate[A]) extends Predicate[A] with Closeable {
    var isClosed = false
    
    override def apply(row: A): Boolean = p(row)
    
    override def close(): Unit = isClosed = true
  }
  
  final case class MockCloseableTransform[A](t: Transform[A]) extends Transform[A] with Closeable {
    var isClosed = false
    
    override def apply(row: A): A = t(row)
    
    override def close(): Unit = isClosed = true
  }
}

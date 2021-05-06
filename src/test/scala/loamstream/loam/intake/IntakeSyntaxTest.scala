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
    
    assert(asCloseable(yIsEven) === Set.empty)
    
    val closeable = MockCloseableRowPredicate(yIsEven)
    
    assert(asCloseable(closeable) === Set(closeable))
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
          
      assert(filtered.rows.records.toList.map(toTuple) === expected)
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
          
      assert(filtered.rows.records.toList.map(toTuple) === expected)
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
          
      assert(filtered.rows.records.toList.map(toTuple) === expected)
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
      
      val source = mapSource.tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).map(expr)
          
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
      
      val source = mapSource.tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).map(expr)
          
      val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
          DestinationParams.To(Helpers.RowSinks.noop),
          metadata,
          source, 
          Fold.foreach(_ => ()),
          Set.empty)
      
      assert(target.toBeClosed === Set.empty)
      
      val unfilteredRows = source.records.toList
      
      val p = MockCloseablePredicate[PValueVariantRow](_.pvalue > 0.2)
      
      val filtered = target.filter(p)
      
      assert(filtered.toBeClosed === Set(p))
      
      val expected = unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2)
      
      assert(filtered.rows.records.toList === expected)
    }
  }
  
  test("MapFilterAndWriteTarget - filter - some rows already skipped") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      
      //Source with first 2 rows already skipped
      val source = {
        val orig = mapSource.tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).map(expr)
        
        val unfilteredRows = orig.records.toList
        
        Source.fromIterable(unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2))
      }
          
      val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
          DestinationParams.To(Helpers.RowSinks.noop), 
          metadata,
          source, 
          Fold.foreach(_ => ()),
          Set.empty)
      
      assert(target.toBeClosed === Set.empty)
      
      val unfilteredRows = source.records.toList
      
      val filtered = target.filter(_.pvalue > 0.3)
      
      val expected = unfilteredRows.take(3).map(_.skip) ++ unfilteredRows.drop(3)
      
      assert(filtered.rows.records.toList === expected)
    }
  }
  
  test("MapFilterAndWriteTarget - map") {
    TestHelpers.withScriptContext { implicit context =>
      import LoamSyntax._
      
      //Source with first 2 rows already skipped
      val source = {
        val orig = mapSource.tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).map(expr)
        
        val unfilteredRows = orig.records.toList
        
        Source.fromIterable(unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2))
      }
          
      val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
          DestinationParams.To(Helpers.RowSinks.noop),
          metadata,
          source, 
          Fold.foreach(_ => ()),
          Set.empty)
      
      assert(target.toBeClosed === Set.empty)
      
      val unmappedRows = source.records.toList
      
      val f: Transform[PValueVariantRow] = MockCloseableTransform { dr => 
        dr.copy(pvalue = dr.pvalue + 1.0)
      }
      
      val mapped = target.map(f)
      
      assert(mapped.toBeClosed === Set(f))
      
      val expected = unmappedRows.take(2) ++ unmappedRows.drop(2).map(_.transform(f))
      
      assert(mapped.rows.records.toList === expected)
    }
  }
  
  test("write - forceLocal=true") {
    TestHelpers.withWorkDir(getClass.getSimpleName) { workDir =>
      
      val outfile = workDir.resolve("out.tsv")
      
      TestHelpers.withScriptContext { implicit context =>
        import LoamSyntax._
        
        //Source with first 2 rows already skipped
        val source = {
          val orig = mapSource.tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).map(expr)
          
          val unfilteredRows = orig.records.toList
          
          Source.fromIterable(unfilteredRows.take(2).map(_.skip) ++ unfilteredRows.drop(2))
        }
            
        val target = new MapFilterAndWriteTarget[PValueVariantRow, Unit](
            DestinationParams.To(RowSink.ToFile(outfile, RowSink.Renderers.csv(Source.Formats.tabDelimited))),
            metadata,
            source, 
            Fold.foreach(_ => ()),
            Set.empty)
        
        assert(target.toBeClosed === Set.empty)
        
        val unmappedRows = source.records.toList
        
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
        
        val source = mapSource.tagFlips(markerDef, Helpers.FlipDetectors.NoFlipsEver).map(expr)
            
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
}

object IntakeSyntaxTest {
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

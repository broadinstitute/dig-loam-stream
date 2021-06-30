package loamstream.loam.intake

import java.io.Closeable
import java.nio.file.Path

import loamstream.compiler.LoamPredef
import loamstream.loam.GraphFunctions
import loamstream.loam.InvokesLsTool
import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.loam.intake.metrics.BioIndexClient
import loamstream.loam.intake.metrics.Metric
import loamstream.loam.intake.metrics.Metrics
import loamstream.model.Store
import loamstream.model.Tool
import loamstream.model.execute.DrmSettings
import loamstream.util.S3Client
import loamstream.util.CanBeClosed
import loamstream.util.CompositeException
import loamstream.util.Files
import loamstream.util.Fold
import loamstream.util.Loggable
import loamstream.util.Terminable
import loamstream.util.Throwables
import loamstream.util.TimeUtils
import scala.collection.compat._


/**
 * @author clint
 * Dec 20, 2019
 */
object IntakeSyntax extends IntakeSyntax {
  object Defaults {
    val bioIndexClient: BioIndexClient = new BioIndexClient.Default()
    
    val numDryRunRows: Int = 10
  }
}

trait IntakeSyntax extends Interpolators with Metrics with RowFilters with RowTransforms with GraphFunctions {
  type ColumnName = loamstream.loam.intake.ColumnName
  val ColumnName = loamstream.loam.intake.ColumnName
  
  type ColumnExpr[A] = loamstream.loam.intake.ColumnExpr[A]
  val ColumnExpr = loamstream.loam.intake.ColumnExpr
  
  type Variant = loamstream.loam.intake.Variant
  val Variant = loamstream.loam.intake.Variant
  
  type AnonColumnDef[A] = loamstream.loam.intake.AnonColumnDef[A]
  val AnonColumnDef = loamstream.loam.intake.AnonColumnDef
  
  type MarkerColumnDef = loamstream.loam.intake.MarkerColumnDef
  val MarkerColumnDef = loamstream.loam.intake.MarkerColumnDef
  
  type Source[A] = loamstream.loam.intake.Source[A]
  val Source = loamstream.loam.intake.Source
  
  type FlipDetector = loamstream.loam.intake.flip.FlipDetector
  val FlipDetector = loamstream.loam.intake.flip.FlipDetector
  
  type AggregatorMetadata = loamstream.loam.intake.AggregatorMetadata
  val AggregatorMetadata = loamstream.loam.intake.AggregatorMetadata
  
  type Row = loamstream.loam.intake.RenderableRow
  
  type HeaderRow = loamstream.loam.intake.LiteralRow
  val HeaderRow = loamstream.loam.intake.LiteralRow
  
  type DataRow = loamstream.loam.intake.DataRow
  val DataRow = loamstream.loam.intake.DataRow
  
  val VariantRow = loamstream.loam.intake.VariantRow
  
  type PValueVariantRow = loamstream.loam.intake.PValueVariantRow
  val PValueVariantRow = loamstream.loam.intake.PValueVariantRow
  
  type VariantCountRow = loamstream.loam.intake.VariantCountRow
  val VariantCountRow = loamstream.loam.intake.VariantCountRow

  type VariantRowExpr[R <: BaseVariantRow] = loamstream.loam.intake.VariantRowExpr[R]
  val VariantRowExpr = loamstream.loam.intake.VariantRowExpr
  
  type PValueVariantRowExpr = loamstream.loam.intake.VariantRowExpr.PValueVariantRowExpr
  val PValueVariantRowExpr = loamstream.loam.intake.VariantRowExpr.PValueVariantRowExpr
  
  type VariantCountRowExpr = loamstream.loam.intake.VariantRowExpr.VariantCountRowExpr
  val VariantCountRowExpr = loamstream.loam.intake.VariantRowExpr.VariantCountRowExpr
  
  val AggregatorColumnDefs = loamstream.loam.intake.AggregatorColumnDefs
  
  val AggregatorColumnNames = loamstream.loam.intake.AggregatorColumnNames
  
  type Ancestry = loamstream.loam.intake.Ancestry
  val Ancestry = loamstream.loam.intake.Ancestry
  
  type TechType = loamstream.loam.intake.TechType
  val TechType = loamstream.loam.intake.TechType
  
  type UploadType = loamstream.loam.intake.UploadType
  val UploadType = loamstream.loam.intake.UploadType
  
  object Log {
    def toFile(store: Store, append: Boolean = false): ToFileLogContext = {
      require(store.isPathStore)
      
      new ToFileLogContext(store.path, append)
    }
  }
  
  private def doLocally[A](body: => A)(implicit scriptContext: LoamScriptContext): NativeTool = {
    LoamPredef.local {
      NativeTool {
        body
      }
    }
  }
  
  private def nativeTool[A](
      forceLocal: Boolean = false)(body: => A)(implicit scriptContext: LoamScriptContext): Tool = { 
    if(forceLocal || scriptContext.lsSettings.thisInstanceIsAWorker) {
      doLocally(body) 
    } else {
      scriptContext.settings match {
        case drmSettings: DrmSettings => InvokesLsTool()
        case settings => {
          sys.error(
              s"Intake jobs can only run locally with --worker or on a DRM system, but settings were $settings")
        }
      }
    }
  }
  
  def requireKnownPhenotype(
      phenotype: Phenotype, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Unit = {
    
    require(bioIndexClient.isKnown(phenotype), s"Phenotype '${phenotype.name}' was not known to the BioIndex")
  }
 
  def requireKnownDataset(
      dataset: Dataset, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Unit = {
    
    require(bioIndexClient.isKnown(dataset), s"Dataset '${dataset.name}' was not known to the BioIndex")
  }
  
  def fixPhenotypeCase(
      phenotype: Phenotype, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Phenotype = {
    
    bioIndexClient.findClosestMatch(phenotype).getOrElse {
      val msg = s"Couldn't find any case-insensitive matches for phenotype '${phenotype.name}' in the BioIndex"
      
      throw new Exception(msg)
    }
  }
  
  def fixDatasetCase(
      dataset: Dataset, 
      bioIndexClient: BioIndexClient = IntakeSyntax.Defaults.bioIndexClient): Dataset = {
    
    bioIndexClient.findClosestMatch(dataset).getOrElse {
      throw new Exception(s"Couldn't find any case-insensitive matches for dataset '${dataset.name}' in the BioIndex")
    }
  }
  
  implicit final class ColumnNameOps(val s: String) {
    def asColumnName: ColumnName = ColumnName(s)
  }
  
  final class TransformationTarget[R <: RenderableJsonRow](destParams: DestinationParams[R]) {
    def from(source: Source[DataRow]): UsingTarget[R] = new UsingTarget(destParams, source)
  }
  
  final class UsingTarget[R <: RenderableJsonRow](
      destParams: DestinationParams[R], 
      rows: Source[DataRow]) extends Loggable {
    
    def using(flipDetector: => FlipDetector): ViaTarget[R] = new ViaTarget(destParams, rows, flipDetector)
  }
  
  private[intake] def asCloseable[A](a: AnyRef): Seq[Closeable] = Option(a).collect { case c: Closeable => c }.to(Seq)
  
  final class ViaTarget[R <: RenderableJsonRow](
      destParams: DestinationParams[R], 
      private[intake] val rows: Source[DataRow],
      flipDetector: => FlipDetector,
      private[intake] val toBeClosed: Set[Closeable] = Set.empty) extends Loggable {
    
    def copy(
      destParams: DestinationParams[R] = this.destParams, 
      rows: Source[DataRow] = this.rows,
      flipDetector: => FlipDetector = this.flipDetector,
      toBeClosed: Set[Closeable] = this.toBeClosed): ViaTarget[R] = new ViaTarget(destParams, rows, flipDetector, toBeClosed) 
    
    private def toFilterTransform(p: DataRowPredicate): DataRow => DataRow = { row =>
      if(row.isSkipped || p(row)) { row } else { row.skip }
    }
    
    def filter(p: DataRowPredicate): ViaTarget[R] = {
      copy(rows = rows.map(toFilterTransform(p)), toBeClosed = toBeClosed ++ asCloseable(p))
    }
    
    def filter(pOpt: Option[DataRowPredicate]): ViaTarget[R] = pOpt match {
      case Some(p) => filter(p)  
      case None => this
    }
   
    private def commitAndClose(metadata: AggregatorMetadata): Unit = {
      import org.json4s._
      
      destParams.close()
    }
    
    def via[R <: BaseVariantRow](expr: VariantRowExpr[R]): MapFilterAndWriteTarget[R, Unit] = {
      val dataRows = rows.tagFlips(expr.markerDef, flipDetector).map(expr)
      
      val pseudoMetric: Metric[R, Unit] = Fold.foreach(_ => ()) // TODO :(
      
      val metadata = expr.metadataWithUploadType
      
      val newDestParams = destParams.withMetadata(metadata)
      
      val newToBeClosed = toBeClosed - destParams + newDestParams
      
      new MapFilterAndWriteTarget(newDestParams, metadata, dataRows, pseudoMetric, newToBeClosed)
    }
  }
  
  final class MapFilterAndWriteTarget[R <: BaseVariantRow, A](
      destParams: DestinationParams[RenderableJsonRow], 
      metadata: AggregatorMetadata,
      private[intake] val rows: Source[VariantRow.Parsed[R]],
      private[intake] val metric: Metric[R, A],
      private[intake] val toBeClosed: Set[Closeable]) extends Loggable {
    
    import loamstream.loam.intake.metrics.MetricOps
    
    def copy(
        destParams: DestinationParams[RenderableJsonRow] = this.destParams, 
        rows: Source[VariantRow.Parsed[R]] = this.rows,
        metric: Metric[R, A] = this.metric,
        toBeClosed: Set[Closeable] = this.toBeClosed): MapFilterAndWriteTarget[R, A] = {
      
      new MapFilterAndWriteTarget(destParams, metadata, rows, metric, toBeClosed)
    }
    
    def writeSummaryStatsTo(store: Store): MapFilterAndWriteTarget[R, (A, Unit)] = {
      require(store.isPathStore)
      
      withMetric(Metric.writeSummaryStatsTo(store.path))
    }
    
    def withMetric[B](m: Metric[R, B]): MapFilterAndWriteTarget[R, (A, B)] = {
      val newMetric = metric combine m
      
      new MapFilterAndWriteTarget[R, (A, B)](destParams, metadata, rows, newMetric, toBeClosed)
    }
    
    def filter(predicate: Predicate[R]): MapFilterAndWriteTarget[R, A] = {
      def filterTransform(row: VariantRow.Parsed[R]): VariantRow.Parsed[R] = row match {
        case t @ VariantRow.Transformed(_, dataRow) => if(predicate(dataRow)) t else t.skip
        case r @ VariantRow.Skipped(_, _, _, _) => r
      }
      
      copy(rows = rows.map(filterTransform), toBeClosed = toBeClosed ++ asCloseable(predicate))
    }
    
    def map(transform: Transform[R]): MapFilterAndWriteTarget[R, A] = {
      //NB: row.transform() is a no-op for skipped rows
      def dataRowTransform(row: VariantRow.Parsed[R]): VariantRow.Parsed[R] = row.transform(transform)
      
      copy(rows = rows.map(dataRowTransform), toBeClosed = toBeClosed ++ asCloseable(transform))
    }
    
    private def toolBody[A](
        message: String = "Uploading", 
        forceLocal: Boolean,
        rowSinkOpt: Option[RowSink[_]])(f: => A)(implicit scriptContext: LoamScriptContext): Tool = {
      
      nativeTool(forceLocal) {
        TimeUtils.time(message, info(_)) {
          CanBeClosed.enclosed(destParams) { _ =>
            CanBeClosed.enclosed(everythingToClose) { _ =>
              f
            }
          }
        }
      }
    }
    
    private def doDryRun(outputDir: Path): (Metric[R, Unit], RowSink[R]) = {
      outputDir.toFile.mkdirs()
      
      val metadataFile = outputDir.resolve("metadata.txt")
      val first10Rows = outputDir.resolve("first10Rows.tsv")
      
      val first10RowsSink = RowSink.ToFile(first10Rows, RowSink.Renderers.csv(Source.Formats.tabDelimited))
      
      val m: Metric[R, Unit] = Metric.writeValidVariantsTo[R](first10RowsSink).map { _ =>
        loamstream.util.Files.writeTo(metadataFile)(metadata.asMetadataFileContents)
      }
      
      (m, first10RowsSink)
    }
    
    //TODO: better name
    def write(
        forceLocal: Boolean = false, 
        dryRun: Boolean = false,
        dryRunOutputDir: Option[Path] = None)(implicit scriptContext: LoamScriptContext): Tool = {
      
      val rowsToProcess = if(dryRun) rows.take(IntakeSyntax.Defaults.numDryRunRows) else rows
      
      def doWrite: (Metric[R, Unit], RowSink[R]) = (dryRun, dryRunOutputDir) match {
        case (true, Some(outputDir)) => doDryRun(outputDir)
        case _ =>  {
          val rowSink = destParams.rowSink
          
          (Metric.writeValidVariantsTo(rowSink), rowSink)
        }
      }
      
      import scala.language.existentials
      val (m: Metric[R, (A, Unit)], rowSink: RowSink[R]) = {
        val (writeMetric, rowSink) = doWrite
        
        (metric.combine(writeMetric), rowSink)
      }
      
      //TODO: How to wire up inputs (if any)?
      //TODO: Better message
      val tool: Tool = toolBody("Uploading", forceLocal, Option(rowSink)) {
        val (metricResults, _) = m.process(rowsToProcess)
      
        //TODO: What to do with metricResults?
      }
      
      addToGraph(tool)
      
      //TODO: Allow S3 Stores
      //TODO: HACK
      rowSink match {
        case RowSink.ToFile(dest, _) => tool.out(LoamPredef.store(dest))
        case _ => ()
      }
      
      tool
    }
    
    private def everythingToClose: Terminable = Terminable {
      val doCloseBlocks: Seq[() => Unit] = toBeClosed.toList.map(closeable => () => closeable.close())
      
      val exceptions = Throwables.collectFailures(doCloseBlocks: _*)
      
      if(exceptions.nonEmpty) {
        throw new CompositeException(exceptions)
      }
    }
  }

  private def requireFsPath(s: Store): Unit = {
    require(s.isPathStore, s"Only writing to a destination on the FS is supported, but got ${s}")
  }
  
  final class AggregatorIntakeConfigFileTarget(dest: Store) {
    def from(
        configData: AggregatorMetadata, 
        forceLocal: Boolean = false)(implicit scriptContext: LoamScriptContext): Tool = {
      
      //TODO: How to wire up inputs (if any)?
      val tool: Tool = nativeTool(forceLocal) {
        Files.writeTo(dest.path)(configData.asMetadataFileContents)
      }
      
      addToGraph(tool)
      
      tool.out(dest)
    }
  }
  
  def produceAggregatorIntakeConfigFile(dest: Store) = {
    requireFsPath(dest)
    
    new AggregatorIntakeConfigFileTarget(dest)
  }
  
  def uploadTo[R <: RenderableJsonRow](uploadParams: DestinationParams[R]): TransformationTarget[R] = {
    new TransformationTarget(uploadParams)
  }
  
  def uploadTo(
      bucketName: String, //TODO: default to real location
      uploadType: UploadType,
      metadata: AggregatorMetadata): TransformationTarget[RenderableJsonRow] = {
    
    uploadTo(DestinationParams.AwsUploadParams(bucketName, uploadType, metadata))
  }
  
  def uploadTo(rowSink: RowSink[RenderableJsonRow]): TransformationTarget[RenderableJsonRow] = {
    uploadTo(DestinationParams.To(rowSink))
  }
  
  sealed trait DestinationParams[+R] extends Closeable {
    def rowSink: RowSink[RenderableJsonRow]
    
    def withMetadata(newMetadata: AggregatorMetadata): DestinationParams[R]
    
    override def close(): Unit = rowSink.close()
  }
  
  object DestinationParams {
    final case class AwsUploadParams[R <: RenderableJsonRow](
      bucketName: String, 
      uploadType: UploadType,
      metadata: AggregatorMetadata) extends DestinationParams[R] {
    
      override def withMetadata(newMetadata: AggregatorMetadata): AwsUploadParams[R] = copy(metadata = newMetadata)
      
      override lazy val rowSink: RowSink[RenderableJsonRow] = {
        val s3Client: S3Client = new S3Client.Default(bucketName)
        
        new AwsRowSink(
            topic = uploadType.s3Dir,
            dataset = metadata.dataset,
            techType = Option(metadata.tech),
            phenotype = Option(metadata.phenotype),
            metadata = metadata.asJObject,
            s3Client = s3Client)
      }
    }
    
    final case class To(val rowSink: RowSink[RenderableJsonRow]) extends DestinationParams[RenderableJsonRow] {
      //TODO: Disallow this
      override def withMetadata(newMetadata: AggregatorMetadata): To = this
    }
  }
}

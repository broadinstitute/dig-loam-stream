package loamstream.loam.intake

import java.io.Closeable
import java.nio.file.Path

import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.S3Config
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId

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
import loamstream.util.AwsClient
import loamstream.util.CanBeClosed
import loamstream.util.CompositeException
import loamstream.util.Files
import loamstream.util.Fold
import loamstream.util.Loggable
import loamstream.util.Terminable
import loamstream.util.Throwables
import loamstream.util.TimeUtils


/**
 * @author clint
 * Dec 20, 2019
 */
object IntakeSyntax extends IntakeSyntax {
  object Defaults {
    val bioIndexClient: BioIndexClient = new BioIndexClient.Default()
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
  
  final class TransformationTarget(rowSink: RowSink[RenderableJsonRow]) {
    def from(source: Source[DataRow]): UsingTarget = new UsingTarget(rowSink, source)
  }
  
  final class UsingTarget(rowSink: RowSink[RenderableJsonRow], rows: Source[DataRow]) extends Loggable {
    def using(flipDetector: => FlipDetector): ViaTarget = new ViaTarget(rowSink, rows, flipDetector)
  }
  
  private[intake] def asCloseable[A](a: AnyRef): Seq[Closeable] = Option(a).collect { case c: Closeable => c }.toSeq
  
  final class ViaTarget(
      rowSink: RowSink[RenderableJsonRow], 
      private[intake] val rows: Source[DataRow],
      flipDetector: => FlipDetector,
      private[intake] val toBeClosed: Seq[Closeable] = Nil) extends Loggable {
    
    def copy(
      dowSink: RowSink[RenderableJsonRow] = this.rowSink, 
      rows: Source[DataRow] = this.rows,
      flipDetector: => FlipDetector = this.flipDetector,
      toBeClosed: Seq[Closeable] = this.toBeClosed): ViaTarget = new ViaTarget(rowSink, rows, flipDetector, toBeClosed) 
    
    private def toFilterTransform(p: DataRowPredicate): DataRow => DataRow = { row =>
      if(row.isSkipped || p(row)) { row } else { row.skip }
    }
    
    def filter(p: DataRowPredicate): ViaTarget = {
      copy(rows = rows.map(toFilterTransform(p)), toBeClosed = asCloseable(p) ++ toBeClosed)
    }
    
    def filter(pOpt: Option[DataRowPredicate]): ViaTarget = pOpt match {
      case Some(p) => filter(p)  
      case None => this
    }
   
    private def commitAndClose(metadata: AggregatorMetadata): Unit = {
      import org.json4s._
      
      rowSink match {
        case aws: AwsRowSink => aws.commit(JObject(metadata.asJson.toList))
        case _ => ()
      }
    }
    
    def via[R <: BaseVariantRow](expr: VariantRowExpr[R]): MapFilterAndWriteTarget[R, Unit] = {
      val dataRows = rows.tagFlips(expr.markerDef, flipDetector).map(expr)
      
      val pseudoMetric: Metric[R, Unit] = Fold.foreach(_ => ()) // TODO :(
      
      val metadata = expr.metadataWithUploadType
      
      val closeRowSink: Closeable = () => commitAndClose(metadata)
      
      new MapFilterAndWriteTarget(rowSink, metadata, dataRows, pseudoMetric, toBeClosed)
    }
  }
  
  final class MapFilterAndWriteTarget[R <: BaseVariantRow, A](
      rowSink: RowSink[RenderableJsonRow], 
      metadata: AggregatorMetadata,
      private[intake] val rows: Source[VariantRow.Parsed[R]],
      private[intake] val metric: Metric[R, A],
      private[intake] val toBeClosed: Seq[Closeable]) extends Loggable {
    
    import loamstream.loam.intake.metrics.MetricOps
    
    def copy(
        rowSink: RowSink[RenderableJsonRow] = this.rowSink, 
        rows: Source[VariantRow.Parsed[R]] = this.rows,
        metric: Metric[R, A] = this.metric,
        toBeClosed: Seq[Closeable] = this.toBeClosed): MapFilterAndWriteTarget[R, A] = {
      
      new MapFilterAndWriteTarget(rowSink, metadata, rows, metric, toBeClosed)
    }
    
    def writeSummaryStatsTo(store: Store): MapFilterAndWriteTarget[R, (A, Unit)] = {
      require(store.isPathStore)
      
      withMetric(Metric.writeSummaryStatsTo(store.path))
    }
    
    def withMetric[B](m: Metric[R, B]): MapFilterAndWriteTarget[R, (A, B)] = {
      val newMetric = metric combine m
      
      new MapFilterAndWriteTarget[R, (A, B)](rowSink, metadata, rows, newMetric, toBeClosed)
    }
    
    def filter(predicate: Predicate[R]): MapFilterAndWriteTarget[R, A] = {
      def filterTransform(row: VariantRow.Parsed[R]): VariantRow.Parsed[R] = row match {
        case t @ VariantRow.Transformed(_, dataRow) => if(predicate(dataRow)) t else t.skip
        case r @ VariantRow.Skipped(_, _, _, _) => r
      }
      
      copy(rows = rows.map(filterTransform), toBeClosed = asCloseable(predicate) ++ toBeClosed)
    }
    
    def map(transform: Transform[R]): MapFilterAndWriteTarget[R, A] = {
      //NB: row.transform() is a no-op for skipped rows
      def dataRowTransform(row: VariantRow.Parsed[R]): VariantRow.Parsed[R] = row.transform(transform)
      
      copy(rows = rows.map(dataRowTransform), toBeClosed = asCloseable(transform) ++ toBeClosed)
    }
    
    private def toolBody[A](
        message: String = "Uploading", 
        forceLocal: Boolean )(f: => A)(implicit scriptContext: LoamScriptContext): Tool = {
      
      nativeTool(forceLocal) {
        TimeUtils.time(message, info(_)) {
          CanBeClosed.enclosed(rowSink) { _ =>
            CanBeClosed.enclosed(everythingToClose) { _ =>
              f
            }
          }
        }
      }
    }
    
    private def doDryRun(outputDir: Path): Metric[R, Unit] = {
      outputDir.toFile.mkdirs()
      
      val metadataFile = outputDir.resolve("metadata.txt")
      val first10Rows = outputDir.resolve("first10Rows.tsv")
      
      val first10RowsSink = RowSink.ToFile(first10Rows, RowSink.Renderers.csv(Source.Formats.tabDelimited))
      
      Metric.writeValidVariantsTo[R](first10RowsSink).map { _ =>
        loamstream.util.Files.writeTo(metadataFile)(metadata.asMetadataFileContents)
      }.map { _ =>
        first10RowsSink.close()
      }
    }
    
    //TODO: better name
    def write(
        forceLocal: Boolean = false, 
        dryRun: Boolean = false,
        dryRunOutputDir: Option[Path] = None)(implicit scriptContext: LoamScriptContext): Tool = {
      
      val rowsToProcess = if(dryRun) rows.take(10) else rows
      
      def writeLines: Metric[R, Unit] = Metric.writeValidVariantsTo(rowSink)
      
      def doWrite: Metric[R, Unit] = (dryRun, dryRunOutputDir) match {
        case (true, Some(outputDir)) => doDryRun(outputDir)
        case _ => writeLines
      }
      
      val m: Metric[R, (A, Unit)] = metric.combine(doWrite)
      
      //TODO: How to wire up inputs (if any)?
      //TODO: Better message
      val tool: Tool = toolBody("Uploading", forceLocal) {
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
      
      //tool.out(dest) 
      
      tool
    }
    
    private def everythingToClose: Terminable = Terminable {
      val doCloseBlocks: Seq[() => Unit] = toBeClosed.map(closeable => () => closeable.close())
      
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
  
  def uploadTo(rowSink: RowSink[RenderableJsonRow]): TransformationTarget = {
    new TransformationTarget(rowSink)
  }
  
  def uploadTo(
      bucketName: String, //TODO: default to real location
      uploadType: UploadType,
      metadata: AggregatorMetadata): TransformationTarget = {
    
    val awsConfig: AWSConfig = {
      //dummy values, except fot the bucket name
      AWSConfig(
          S3Config(bucketName), 
          EmrConfig("some-ssh-key-name", SubnetId("subnet-foo")))
    }
  
    val awsClient: AwsClient = new AwsClient.Default(new AWS(awsConfig))
    
    val rowSink: RowSink[RenderableJsonRow] = new AwsRowSink(
        topic = uploadType.s3Dir,
        dataset = metadata.dataset,
        techType = Option(metadata.tech),
        phenotype = Option(metadata.phenotype),
        awsClient = awsClient,
        yes = true)
    
    uploadTo(rowSink)
  }
  
  
}

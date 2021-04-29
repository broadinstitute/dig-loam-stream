

import scala.concurrent.Future
import scala.util.Success

import org.broadinstitute.dig.aws.AWS
import org.broadinstitute.dig.aws.config.AWSConfig
import org.broadinstitute.dig.aws.config.S3Config
import org.broadinstitute.dig.aws.config.emr.EmrConfig
import org.broadinstitute.dig.aws.config.emr.SubnetId

import loamstream.loam.LoamScriptContext
import loamstream.loam.NativeTool
import loamstream.util.AwsClient
import loamstream.util.HttpClient.Auth
import loamstream.loam.intake.Source
import loamstream.util.Fold
import loamstream.util.CanBeClosed
import java.nio.file.Files
import java.nio.file.OpenOption
import java.nio.file.StandardOpenOption
import java.io.PrintWriter
import loamstream.util.Maps
import loamstream.loam.intake.dga.DgaSyntax
import loamstream.loam.intake.dga.Annotation
import loamstream.loam.intake.dga.BedRow
import loamstream.loam.intake.dga.Strand
import loamstream.loam.intake.dga.BedRowExpr
import loamstream.loam.intake.DataRowParser
import scala.util.Try
import loamstream.loam.intake.DataRow
import loamstream.loam.intake.dga.BedRowExpr
import java.nio.file.Files
import loamstream.util.CanBeClosed
import scala.meta.parsers.Parse
import loamstream.util.TimeUtils
import loamstream.util.CanBeClosed

object AnalyzeStrands extends loamstream.LoamFile {
  import DgaSyntax._
  
  //TODO
  val auth: Auth = Auth(username = "BA5V5EID", password = "tr4i3skb5yquihn2")
    
  val awsClient = {
    //TODO
    //val bucketName: String = "dig-integration-tests"
    val bucketName: String = "dig-analysis-data"

    val awsConfig: AWSConfig = {
      //dummy values, except for the bucket name
      AWSConfig(
          S3Config(bucketName), 
          EmrConfig("some-ssh-key-name", SubnetId("subnet-foo"))) 
    }
    
    val aws: AWS = new AWS(awsConfig)    
    
    new AwsClient.Default(aws)
  }
    
  val annotations: Source[Try[Annotation]] = {
    /*CanBeClosed.using(Files.newBufferedReader(path("annotations.json"))) { reader =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      import org.json4s.JsonAST.JArray
      
      val anns = (parse(reader) \ "11") match {
        case JArray(annotationBlocks) => annotationBlocks
        case _ => throw new Exception("Couldn't parse DGA JSON")
      }
      
      Source.fromIterable(anns).map(Annotation.fromJson(Map.empty))
    }*/
    
    Dga.Annotations.downloadAnnotations()
  }
  
  {
    val (successes, failures) = annotations.records.toList.partition(_.isSuccess)
    
    println(s"%%%%%%%%% ${successes.size} successes")
    println(s"%%%%%%%%% ${failures.size} failures")
  }
  
  annotations.records.map(_.get).toList
  
  val strandCountsStore = store("./out/strand-counts")
  val ignoredAnnotationsStore = store("./out/ignored-annotations")
  val failedToParseAnnotationsStore = store("./out/bad-annotations")
  val badBedRowsStore = store("./out/bad-bed-rows")
  val missingBedStore = store("./out/missing-bed-files")
    
  private type Counts[E] = Map[E, Int]
  
  //TODO: Extract
  private def countByStrand[E](z: Counts[E] = Map.empty): Fold[E, _, Counts[E]] = {
    def doAdd(acc: Counts[E], elem: E): Counts[E] = {
      val newCount = acc.get(elem) match {
        case Some(count) => count + 1
        case None => 1
      }
        
      acc + (elem -> newCount)
    }
    
    Fold.apply[E, Counts[E], Counts[E]](z, doAdd, identity)
  }
  
  def strandExpr(annotation: Annotation): DataRowParser[Try[Option[Strand]]] = new DataRowParser[Try[Option[Strand]]] {
    private val strand = (new BedRowExpr.Columns(annotation)).strand
    
    override def apply(row: DataRow): Try[Option[Strand]] = Try(strand(row))
  }
  
  val rowSourcesByAnnotation = annotations.
    filter(Dga.Annotations.Predicates.succeeded(failedToParseAnnotationsStore, append = true)).
    collect { case Success(a) => a }.
    filter(Dga.Annotations.Predicates.isUploadable(ignoredAnnotationsStore, append = true)).
    //filter(Dga.Annotations.Predicates.hasAnnotationTypeWeCareAbout(ignoredAnnotationsStore, append = true)).
    filter(Dga.Annotations.Predicates.hasAnyBeds(logTo = missingBedStore, append = true, auth = Some(auth))).
    flatMap(a => Source.fromIterable(a.downloads.map(d => a -> d)))
    .map { case (annotation, download) => annotation -> Dga.Beds.downloadBed(download.url, auth = None) }
    //.collect { case (annotation, Success(bedRows)) => annotation -> bedRows }
    .map { case (annotation, bedRows) => annotation -> bedRows.map(strandExpr(annotation)).collect { case Success(r) => r }}
  
  val parallelism: Int = System.getProperty("DGA_INTAKE_PARALLELISM", "1").toInt
  
  def makeWriterAndWriteHeader(): PrintWriter = {
    val writer = {
      new PrintWriter(Files.newBufferedWriter(strandCountsStore.path, StandardOpenOption.CREATE, StandardOpenOption.APPEND))
    }
    
    writer
  }

  val writer = makeWriterAndWriteHeader()

  writer.println(s"ANNOTATION_ID${'\t'}+${'\t'}-${'\t'}NA")
  writer.flush()
  
  val tool: Tool = doLocally {
    val firstN = rowSourcesByAnnotation.records.grouped(parallelism).foreach { annotations =>
      import scala.concurrent.ExecutionContext.Implicits.global
      
      val z: Counts[Option[Strand]] = Iterator(Some(Strand.+), Some(Strand.-), None).map(_ -> 0).toMap
      
      val fs = for {
        (annotation, strandOpts) <- annotations
      } yield {
        Future {
          val (Success((counts, numRows)), elapsedMillis) = TimeUtils.elapsed {
            (countByStrand(z) |+| Fold.count).process(strandOpts.records)
          }
          
          info(s"Analyzing ${numRows} from ${annotation.annotationId} took ${elapsedMillis}ms (${numRows.toDouble / (elapsedMillis.toDouble / 1000.0)}) rows/s")
          
          def anyMinuses(counts: Counts[Option[Strand]]): Boolean = counts.get(Option(Strand.-)).map(_ > 0).getOrElse(false)
          def anyPluses(counts: Counts[Option[Strand]]): Boolean = counts.get(Option(Strand.+)).map(_ > 0).getOrElse(false)
          
          val anyPlusesOrMinuses = anyMinuses(counts) || anyPluses(counts)
          
          def toString(counts: Counts[Option[Strand]]): String = {
            val plusCount = counts.get(Option(Strand.+)).getOrElse(0)
            val minusCount = counts.get(Option(Strand.-)).getOrElse(0)
            val naCount = counts.get(None).getOrElse(0)
            
            s"${plusCount}${'\t'}${minusCount}${'\t'}${naCount}"
          }
          
          def toLine(a: Annotation, counts: Counts[Option[Strand]]): String = {
            s"${annotation.annotationId}${'\t'}${toString(counts)}"
          }
          
          //CanBeClosed.using(makeWriterAndWriteHeader()) { writer =>
            //if(anyPlusesOrMinuses) {
              writer.println(toLine(annotation, counts))
              writer.flush()
            //}
          //}
        }
      }
      
      scala.concurrent.Await.result(Future.sequence(fs), scala.concurrent.duration.Duration.Inf)
      
    }
    
    writer.close()
  }
  
  addToGraph(tool)
  
  private def doLocally[A](body: => A)(implicit scriptContext: LoamScriptContext): NativeTool = {
    local {
      NativeTool {
        body
      }
    }
  }
}
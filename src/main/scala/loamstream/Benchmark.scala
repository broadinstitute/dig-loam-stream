/*package loamstream

import loamstream.loam.intake.IntakeSyntax
import java.nio.file.Paths
import loamstream.loam.intake.flip.Disposition
import loamstream.loam.intake.RowSink
import squants.time.TimeUnit
import loamstream.util.TimeUtils
import loamstream.loam.intake.metrics.Metric
import java.nio.file.Files
import loamstream.util.CanBeClosed.using
import java.util.zip.GZIPInputStream
import java.io.InputStreamReader
import java.io.FileInputStream
import java.io.BufferedReader

object Benchmark extends App with IntakeSyntax {
  val inFile = Paths.get("/home/clint/workspace/dig-loam-stream/in2.tsv.gz")

  val source = Source.fromGzippedFile(inFile, Source.Formats.tabDelimitedWithHeader)
  
  object FlipDetectors {
    object NoFlipsEver extends FlipDetector {
      override def isFlipped(variantId: Variant): Disposition = Disposition.NotFlippedSameStrand
    }
  }
  
  val POS = ColumnName("POS")
  val CHROM = ColumnName("CHROM")
  val REF = ColumnName("REF")
  val ALT = ColumnName("ALT")
  val PVALUE = ColumnName("PVALUE")
  val ZSCORE = ColumnName("ZSCORE")
  val STDERR = ColumnName("STDERR")
  val BETA = ColumnName("BETA")
  val EAF = ColumnName("EAF")
  val MAF = ColumnName("MAF")
  
  val markerDef = AggregatorColumnDefs.marker(chromColumn = CHROM, posColumn = POS, refColumn = REF, altColumn = ALT)
  
  val toAggregatorFormat = AggregatorRowExpr(
      markerDef = markerDef,
      pvalueDef = AggregatorColumnDefs.pvalue(PVALUE),
      //zscoreDef = Some(AggregatorColumnDefs.zscore(ZSCORE)),
      //stderrDef = Some(AggregatorColumnDefs.stderr(STDERR)),
      betaDef = Some(AggregatorColumnDefs.beta(BETA)),
      eafDef = Some(AggregatorColumnDefs.eaf(EAF)),
      mafDef = Some(AggregatorColumnDefs.eaf(MAF, AggregatorColumnNames.maf)),
      failFast = true )
  
  val n = 1000000
      
  val transformed = source.take(n).tagFlips(markerDef, FlipDetectors.NoFlipsEver).map(toAggregatorFormat)
  
  def records = transformed.filter(_.notSkipped).records 
  
  {
    val outFile = Paths.get("./out0")

    TimeUtils.time(s"Copying lines from $inFile to $outFile", println) {
      using(new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(inFile.toFile))))) { reader =>
        using(Files.newBufferedWriter(outFile)) { writer =>
          reader.lines.forEach(writer.write)
        }
      }
    }
  }
  
  {
    val outFile = Paths.get("./out1")
    
    val (_, elapsed) = TimeUtils.elapsed {
      println(s"Processing rows from $inFile, building $outFile")
      
      using(RowSink.ToFile(outFile)) { sink =>
        Metric.writeValidVariantsTo(sink).process(records)
      }
    }
    
    println(s"Processing $n records took ${elapsed / 1000.0}s - ${n.toDouble / (elapsed.toDouble / 1000.0)} records/s")
  }
  
  {
    val outFile = Paths.get("./out1")
    
    val (_, elapsed) = TimeUtils.elapsed {
      println(s"Processing rows from $inFile, building $outFile, computing summary stats in 1 pass")
      
      using(RowSink.ToFile(outFile)) { sink =>
        Metric.writeValidVariantsTo(sink).process(records)
        
        Metric.countNOTSkipped.process(records) 
        Metric.countSkipped.process(records) 
        Metric.countFlipped.process(records) 
        Metric.countComplemented.process(records) 
        Metric.countByChromosome(countSkipped = false).process(records) 
      }
    }
    
    println(s"Processing $n records took ${elapsed / 1000.0}s - ${n.toDouble / (elapsed.toDouble / 1000.0)} records/s")
  }
  
  
}
*/

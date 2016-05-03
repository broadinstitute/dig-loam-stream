package loamstream.io.rdf

import java.io.FileOutputStream
import java.nio.file.{Path, Paths}

import loamstream.apps.hail.HailPipeline
import loamstream.apps.minimal.MiniPipeline
import loamstream.model.LPipeline
import loamstream.pipelines.qc.ancestry.AncestryInferencePipeline
import loamstream.tools.core.LCoreDefaultStoreIds
import org.openrdf.rio.helpers.{BasicWriterSettings, BufferedGroupingRDFHandler}
import org.openrdf.rio.{RDFFormat, Rio}

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 4/27/16.
  */
object PipelineWriteApp extends App {

  def writePipeline(pipeline: LPipeline, path: Path): Unit = {
    val repo = SesameRepo.inMemory
    val conn = repo.getConnection
    val redFern = RedFern(conn)
    redFern.write(pipeline)(LPipelineEncoder)
    var writer = Rio.createWriter(RDFFormat.TURTLE, new FileOutputStream(path.toFile))
    writer = writer.set[java.lang.Boolean](BasicWriterSettings.PRETTY_PRINT, true)
    val bufferedHandler = new BufferedGroupingRDFHandler(writer)
    conn.export(bufferedHandler)
    conn.close()
  }

  writePipeline(MiniPipeline(LCoreDefaultStoreIds.genotypes), Paths.get("mini.ttl"))
  writePipeline(HailPipeline(LCoreDefaultStoreIds.genotypes, LCoreDefaultStoreIds.vds,
    LCoreDefaultStoreIds.singletons), Paths.get("hail.ttl"))
  writePipeline(AncestryInferencePipeline(LCoreDefaultStoreIds.genotypes, LCoreDefaultStoreIds.pcaWeights),
    Paths.get("ancestry.ttl"))
}

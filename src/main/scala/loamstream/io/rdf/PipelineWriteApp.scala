package loamstream.io.rdf

import java.io.FileOutputStream
import java.nio.file.{Path, Paths}

import loamstream.apps.minimal.MiniPipeline
import loamstream.model.LPipeline
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
    val writer = Rio.createWriter(RDFFormat.TURTLE, new FileOutputStream(path.toFile))
    conn.export(writer)
    conn.close()
  }

  val file = Paths.get("mini.rdf")
  writePipeline(MiniPipeline.apply("genotypes"), file)
}

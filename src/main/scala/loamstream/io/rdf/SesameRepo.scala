package loamstream.io.rdf

import org.openrdf.model.vocabulary.{RDF, RDFS, XMLSchema}
import org.openrdf.repository.{Repository, RepositoryConnection}
import org.openrdf.repository.sail.SailRepository
import org.openrdf.sail.memory.MemoryStore

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 4/27/16.
  */
object SesameRepo {

  def addNamespaces(conn: RepositoryConnection): Unit = {
    conn.setNamespace("", Loam.namespace)
    conn.setNamespace("rdf", RDF.NAMESPACE)
    conn.setNamespace("rdfs", RDFS.NAMESPACE)
    conn.setNamespace("xsd", XMLSchema.NAMESPACE)
    conn.setNamespace("store", LIdIris.storesNamespace)
    conn.setNamespace("tool", LIdIris.toolsNamespace)
  }

  def inMemory: Repository = {
    val repo = new SailRepository(new MemoryStore())
    repo.initialize()
    addNamespaces(repo.getConnection)
    repo
  }

}

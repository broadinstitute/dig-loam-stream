package loamstream.io.rdf

import org.openrdf.repository.Repository
import org.openrdf.repository.sail.SailRepository
import org.openrdf.sail.memory.MemoryStore

/**
  * LoamStream - Language for Omics Analysis Management
  * Created by oruebenacker on 4/27/16.
  */
object SesameRepo {

  def inMemory: Repository = {
    val repo = new SailRepository(new MemoryStore())
    repo.initialize()
    repo
  }

}

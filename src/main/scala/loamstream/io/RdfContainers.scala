package loamstream.io

import org.openrdf.model.IRI
import org.openrdf.model.vocabulary.RDF
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/15/2016.
  */
object RdfContainers {

  def membershipProperty(i: Int)(implicit conn: RepositoryConnection): IRI =
    conn.getValueFactory.createIRI(RDF.NS.getName, s"_$i")

  val _1 = membershipProperty(1)
  val _2 = membershipProperty(2)

}

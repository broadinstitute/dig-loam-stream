package loamstream.io.rdf

import loamstream.util.{Hit, Miss, Shot}
import org.openrdf.model.{IRI, Resource, Statement, Value}
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/15/2016.
  */
object RdfQueries {

  val anyValue: IRI = null // scalastyle:ignore null

  def findUniqueStatement(subj: Resource, pred: IRI, obj: Value,
                          contexts: Resource*)(implicit conn: RepositoryConnection):
  Shot[Statement] = {
    val repoResult = conn.getStatements(subj, pred, obj, contexts.toArray: _*)
    if (repoResult.hasNext) {
      val firstStatement = repoResult.next()
      if (repoResult.hasNext) {
        val contextsString = contexts.mkString("[", ", ", "]")
        Miss(s"Found more than one statement matching pattern ($subj, $pred, $obj, $contextsString).")
      } else {
        Hit(firstStatement)
      }
    } else {
      val contextsString = contexts.mkString("[", ", ", "]")
      Miss(s"Could not find statement matching pattern ($subj, $pred, $obj, $contextsString).")
    }
  }

  def findUniqueObject(subj: Resource, pred: IRI,
                       contexts: Resource*)(implicit conn: RepositoryConnection): Shot[Value] =
    findUniqueStatement(subj, pred, anyValue, contexts: _*).map(_.getObject)

}

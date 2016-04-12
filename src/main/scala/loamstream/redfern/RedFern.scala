package loamstream.redfern

import loamstream.util.shot.Shot
import org.openrdf.model.Resource
import org.openrdf.repository.RepositoryConnection

/**
  * LoamStream
  * Created by oliverr on 4/12/2016.
  */
object RedFern {
  def getNew(implicit conn: RepositoryConnection): RedFern = RedFern(conn)
}

case class RedFern(conn: RepositoryConnection) {

  def write[T](root: Resource, thing: T)(implicit encoder: Encoder[T]): Unit = encoder.encode(this, root, thing)

  def read[T](root: Resource)(implicit decoder: Decoder[T]): Shot[T] = decoder.read(this, root)

}

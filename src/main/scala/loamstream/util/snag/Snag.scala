package loamstream.util.snag

/**
  * LoamStream
  * Created by oliverr on 11/17/2015.
  */
object Snag {
  def apply(message: String): Snag = SnagAtom(message)

  def apply(child: Snag, children: Snag*): Snag = SnagSeq(children)

  def apply(message: String, children: Snag*): Snag = SnagTree(message, children)
}

trait Snag {
  def message: String

  def children: Seq[Snag]

  def ++(snag: Snag): SnagSeq = {
    snag match {
      case SnagSeq(snags) => SnagSeq(this +: snags)
      case _ => SnagSeq(Seq(this, snag))
    }
  }
}

case class SnagAtom(message: String) extends Snag {
  override def children: Seq[Snag] = Seq.empty[Snag]
}

case class SnagSeq(children: Seq[Snag]) extends Snag {
  override def message: String = "" + children.size + " snags."

  override def ++(snag: Snag): SnagSeq = {
    snag match {
      case SnagSeq(snags) => SnagSeq(children ++ snags)
      case _ => SnagSeq(children :+ snag)
    }
  }
}

case class SnagTree(message: String, children: Seq[Snag]) extends Snag {

}
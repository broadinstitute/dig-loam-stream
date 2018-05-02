package loamstream.util

object Gensym {
  private val i = Iterator.from(1)

  /** Generates a unique identifier with a 5-digit index. */
  def apply(prefix: String = "gensym") = s"${prefix}_${i.next}"
}

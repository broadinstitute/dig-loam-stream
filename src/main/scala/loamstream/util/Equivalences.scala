package loamstream.util

/** Stores stated equivalences between objects, with transitive reasoning */
trait Equivalences[E] {
  /** Creates a new Equivalences which holds e1 and e2 equal */
  def withTheseEqual(e1: E, e2: E): Equivalences[E]

  /** Returns whether e1 and e2 are considered equal */
  def theseAreEqual(e1: E, e2: E): Boolean

  /** Returns set of all elements equal to e, including e. */
  def equalsOf(e: E): Set[E]
}

object Equivalences {
  /** An empty equivalences which holds no two objects equal */
  def empty[E]: Equivalences[E] = MapEquivalences(Map.empty)

  /** An equivalences that stores a set of equals for each object */
  final case class MapEquivalences[E](equals: Map[E, Set[E]]) extends Equivalences[E] {
    /** Creates a new Equivalencer which holds e1 and e2 equal */
    override def withTheseEqual(e1: E, e2: E): Equivalences[E] = {
      val equalsOfThese = equalsOf(e1) ++ equalsOf(e2)
      val updatedEquals = equalsOfThese.map(e => (e, equalsOfThese)).toMap
      MapEquivalences(equals ++ updatedEquals)
    }

    /** Returns whether e1 and e2 are considered equal */
    override def theseAreEqual(e1: E, e2: E): Boolean = equalsOf(e1)(e2)

    /** Returns set of all elements equal to e, including e. */
    override def equalsOf(e: E): Set[E] = equals.getOrElse(e, Set(e))
  }

}

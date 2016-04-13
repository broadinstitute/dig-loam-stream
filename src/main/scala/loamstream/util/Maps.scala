package loamstream.util

object Maps {
  def mergeMaps[A, B](maps: TraversableOnce[Map[A, B]]): Map[A, B] = {
    val z: Map[A, B] = Map.empty

    maps.foldLeft(z)(_ ++ _)
  }
}
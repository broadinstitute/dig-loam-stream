package loamstream.util.shot

/**
  * LoamStream
  * Created by oliverr on 4/14/2016.
  */
object Shots {

  def unpack[E](shots: Iterable[Shot[E]]): Shot[Iterable[E]] = {
    val misses = shots.collect({ case miss: Miss => miss })
    if (misses.isEmpty) {
      Hit(shots.map(_.get))
    } else {
      Miss(misses.map(_.snag).reduce(_ ++ _))
    }
  }

}

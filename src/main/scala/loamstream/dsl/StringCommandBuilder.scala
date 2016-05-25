package loamstream.dsl

/**
  * LoamStream
  * Created by oliverr on 5/25/2016.
  */
object StringCommandBuilder {

  implicit class StringContextWithCmd(val stringContext: StringContext) extends AnyVal {
    def cmd(args: Int*): Unit = println(args.mkString("[", "][", "]")) // scalastyle:ignore
  }

}


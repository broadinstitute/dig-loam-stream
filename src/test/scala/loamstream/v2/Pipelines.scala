package loamstream.v2

import loamstream.TestHelpers.path
import java.nio.file.Path
import loamstream.util.Files
import loamstream.util.Paths.Implicits._

object Pipelines {
  def oneCommand(outputDir: Path): Context = withContext { implicit context =>
    import V2Predef._

    val foo = store(outputDir / "foo.txt")

    val bar = store(outputDir / "bar.txt")

    cmd"cp $foo $bar".in(foo).out(bar)
  }

  def simpleScatter(outputDir: Path): Context = withContext { implicit context =>
    import V2Predef._

    val foo = store(outputDir / "foo.txt")

    val bar = store(outputDir / "bar.txt")

    cmd"cp $foo $bar".in(foo).out(bar).named("foo2bar")

    def countLines(s: Store) = lift(Files.countLines)(s)

    val count = value(countLines(bar).toInt)

    val bazes = loop(count) { i =>
      val out = store(outputDir / s"baz-$i.txt")

      val content = i.toString * (i + 1)

      cmd"echo $content > $out".out(out).named(s"baz-$i.txt")

      out
    }

    bazes.in(count)
  }

  def scatterGather(outputDir: Path): Context = withContext { implicit context =>
    import V2Predef._

    val foo = store(outputDir / "foo.txt")

    val bar = store(outputDir / "bar.txt")

    val blerg = store(outputDir / "blerg.txt")

    cmd"cp $foo $bar".in(foo).out(bar)

    def countLines(s: Store) = lift(Files.countLines)(s)

    val count = value(countLines(bar).toInt)

    val bazes = loop(count) { i =>
      val out = store(outputDir / s"baz-$i.txt")

      val content = i.toString * (i + 1)

      cmd"echo $content > $out".out(out)

      out
    }

    bazes.in(count)

    val gatherStep = bazes.map { stores =>
      cmd"(cat $stores | sort) > $blerg".in(stores).out(blerg)
    }
  }

  def linearWithDataDeps(outputDir: Path): Context = withContext { implicit context =>
    import V2Predef._

    val foo = store(outputDir / "foo.txt")

    val bar = store(outputDir / "bar.txt")

    val baz = store(outputDir / "baz.txt")

    def countLines(s: Store) = {
      import scala.collection.JavaConverters._

      def count(p: Path) = java.nio.file.Files.lines(p).iterator.asScala.filter(_.trim.nonEmpty).size

      lift(count)(s)
    }

    val fooCount = value(countLines(foo).toInt)

    val barCount = value(countLines(bar).toInt)

    val barCountTimesTwo = barCount.map(_ * 2)

    cmd"""str=""
          for i in {1..$fooCount} 
          do 
            str="$${str}bar-$${i}\n" 
          done  
          echo -e "$${str}" > $bar""".in(foo).out(bar)
          
    cmd"""str=""
          for i in {1..$barCountTimesTwo} 
          do 
            str="$${str}baz-$${i}\n" 
          done  
          echo -e "$${str}" > $baz""".in(bar).out(baz)
  }

  def linear(outputDir: Path): Context = withContext { implicit context =>
    import V2Predef._

    val foo = store(outputDir / "foo.txt")

    val bar = store(outputDir / "bar.txt")

    val baz = store(outputDir / "baz.txt")

    val blerg = store(outputDir / "blerg.txt")

    cmd"cp $foo $bar".in(foo).out(bar)
    cmd"cp $bar $baz".in(bar).out(baz)
    cmd"cp $baz $blerg".in(baz).out(blerg)
  }

  private def withContext(f: Context => Any): Context = {
    val context = new Context

    f(context)

    context
  }
}

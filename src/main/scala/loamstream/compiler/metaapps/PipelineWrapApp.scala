package loamstream.compiler.metaapps

import java.nio.file.Paths

/**
 * LoamStream
 * Created by oliverr on 10/7/2015.
 */
object PipelineWrapApp extends App {

  val inFile = Paths.get( """C:\Users\oliverr\pipelines\old\real\targeted.cfg""")
  val outDir = Paths.get( """C:\Users\oliverr\git\dig-loam-stream\src\main\scala\loamstream\compiler""")
  val tripleQuotes = "\"\"\""
  val tag = "XKCD"
  val fileName = s"PipelineTargetedPart$tag.scala"
  val prefix =
    s"""
       |package loamstream.compiler
       |
       |/**
       | * LoamStream
       | * Created by oliverr on 10/6/2015.
       | */
       |object PipelineTargetedPart$tag {
       |  val string =
       | """.stripMargin + tripleQuotes
  val suffix = tripleQuotes +
    """
      |}
    """.stripMargin

  val fileStringsTemplate = FileChunkWrapper.FileStringsTemplate(fileName, prefix, suffix, tag)

  FileChunkWrapper.wrap(inFile, outDir, fileStringsTemplate)

}

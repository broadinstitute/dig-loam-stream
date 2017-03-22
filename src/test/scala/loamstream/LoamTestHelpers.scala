package loamstream

import java.nio.file.Path
import java.nio.file.Paths

import loamstream.compiler.LoamCompiler
import loamstream.compiler.LoamProject
import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.loam.LoamProjectContext
import loamstream.loam.LoamScript
import loamstream.loam.LoamToolBox
import loamstream.loam.ast.LoamGraphAstMapper
import loamstream.loam.ast.LoamGraphAstMapping
import loamstream.model.execute.Executable
import loamstream.model.execute.RxExecuter
import loamstream.model.jobs.{JobResult, LJob}
import loamstream.util.Loggable


/**
  * @author clint
  *         date: Jul 8, 2016
  */
trait LoamTestHelpers extends Loggable {

  def compileFile(file: String): LoamCompiler.Result = compile(Paths.get(file))

  def compile(path: Path, rest: Path*): LoamCompiler.Result = {
    def toScript(p: Path): LoamScript = LoamScript.read(p).get
    
    val paths: Set[Path] = (path +: rest).toSet
    
    compile(LoamProject(TestHelpers.config, paths.map(toScript)), throwOnError = false)
  }

  def compile(script: LoamScript): LoamCompiler.Result = compile(LoamProject(TestHelpers.config, Set(script)))
    
  def compile(project: LoamProject, throwOnError: Boolean = true): LoamCompiler.Result = {

    val compiler = new LoamCompiler(LoamCompiler.Settings.default, LoggableOutMessageSink(this))

    val compileResults = compiler.compile(project)

    if(throwOnError) {
      if (!compileResults.isValid) {
        throw new IllegalArgumentException(s"Could not compile '$project': ${compileResults.errors}.")
      }
    }
    
    compileResults
  }

  def toExecutable(compileResults: LoamCompiler.Result): (LoamGraphAstMapping, Executable) = {
    val context: LoamProjectContext = compileResults.contextOpt.get
    val graph = context.graph

    val mapping = LoamGraphAstMapper.newMapping(graph)

    val toolBox = new LoamToolBox(context)

    val executable = mapping.rootAsts.map(toolBox.createExecutable).reduce(_ ++ _)

    (mapping, executable)
  }

  def run(executable: Executable): Map[LJob, JobResult] = RxExecuter.default.execute(executable)
}

package loamstream.loam

import java.nio.file.{Path, Paths}

import loamstream.util.{DepositBox, ValueBox}

/** Container for compile time and run time context for a script */
class LoamScriptContext(val projectContext: LoamProjectContext) {

  val workDirBox: ValueBox[Path] = ValueBox(Paths.get("."))

  def workDir: Path = workDirBox.value

  def setWorkDir(newDir: Path): Path = {
    workDirBox.value = newDir
    newDir
  }

  def changeWorkDir(newDir: Path): Path = workDirBox.mutate(_.resolve(newDir)).value

}

/** Container for compile time and run time context for a script */
object LoamScriptContext {
  def fromDepositedProjectContext(receipt: DepositBox.Receipt): LoamScriptContext =
    new LoamScriptContext(LoamProjectContext.depositBox(receipt))
}
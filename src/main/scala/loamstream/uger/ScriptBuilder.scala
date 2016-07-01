package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineBuilderJob

/**
  * @author Kaan
  *         Date: Jul 1, 2016
  *
  * Used to facilitate generation of bash scripts to submit task arrays to UGER
  * For an example of such scripts, see src/test/resources/imputation/shapeItUgerSubmissionScript.sh
  */
object ScriptBuilder {
  val tab: String = "\t"
  val newLine: String = "\n"
  val unixLineSep: String = " \\"

  val scriptHeader: String =
    s"""#!/bin/bash
#$$ -cwd
#$$ -j y

source /broad/software/scripts/useuse
reuse -q UGER

i=$$SGE_TASK_ID
      """
  val endIf: String = s"""${newLine}fi$newLine"""

  def buildFrom(commandLineBuilderJobs: Seq[CommandLineBuilderJob]): String = {
    val taskIndexStartValue = 1
    val firstIfBlock = getFirstIfBlock(commandLineBuilderJobs.head, taskIndexStartValue)

    val elseIfBlocks = commandLineBuilderJobs.tail.zipWithIndex.map({case (job, index) =>
      s"${getElseIfHeader(index + 2)}${getBody(job, s"$newLine$tab")}"}).mkString(newLine)

    s"$scriptHeader$newLine$firstIfBlock$newLine$elseIfBlocks$endIf"
  }

  def getFirstIfBlock(commandLineBuilderJob: CommandLineBuilderJob, indexStartValue: Int): String = {
    val ifHeader = getIfHeader(indexStartValue)
    val ifBody = getBody(commandLineBuilderJob, s"$newLine$tab")

    s"$ifHeader$ifBody"
  }

  def getBody(commandLineBuilderJob: CommandLineBuilderJob, sep: String): String = {
    s"""$newLine$tab${commandLineBuilderJob.commandLine.tokens.mkString(s"$unixLineSep$sep")}$unixLineSep"""
  }

  def getIfHeader(index: Int): String = {
    s"""if [ $$i -eq $index ]${newLine}then"""
  }

  def getElseIfHeader(index: Int): String = {
    s"""elif [ $$i -eq $index ]${newLine}then"""
  }
}

package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob

/**
  * @author Kaan
  *         Date: Jul 1, 2016
  *
  * Used to facilitate generation of bash scripts to submit task arrays to UGER
  * For an example of such scripts, see src/test/resources/imputation/shapeItUgerSubmissionScript.sh
  */
private[uger] object ScriptBuilder {
  val space: String = " "
  val tab: String = "\t"
  val newLine: String = "\n"
  val unixLineSep: String = " \\"

  //NB: We need to 'use' Java-1.8 to make some steps of the QC pipeline work.  
  val scriptHeader: String =
    s"""#!/bin/bash
#$$ -cwd
#$$ -j y

source /broad/software/scripts/useuse
reuse -q UGER
reuse -q Java-1.8

export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$$PATH
conda env create -f /humgen/diabetes/users/dig/hail/environment.yml
source activate hail

i=$$SGE_TASK_ID
      """
  val endIf: String = s"""${newLine}fi$newLine"""

  def buildFrom(commandLineJobs: Seq[CommandLineJob]): String = {
    val taskIndexStartValue = 1
    val firstIfBlock = getFirstIfBlock(commandLineJobs.head, taskIndexStartValue)

    val elseIfBlocks = commandLineJobs.tail.zipWithIndex.map { case (job, index) =>
      s"${getElseIfHeader(index + 2)}${getBody(job)}"
    }.mkString(newLine)

    s"$scriptHeader$newLine$firstIfBlock$newLine$elseIfBlocks$endIf"
  }

  def getFirstIfBlock(commandLineJob: CommandLineJob, indexStartValue: Int): String = {
    val ifHeader = getIfHeader(indexStartValue)
    val ifBody = getBody(commandLineJob)

    s"$ifHeader$ifBody"
  }

  def getBody(commandLineJob: CommandLineJob): String = {
    s"""$newLine$tab${commandLineJob.commandLineString}"""
  }

  def getIfHeader(index: Int): String = s"""if [ $$i -eq $index ]${newLine}then"""

  def getElseIfHeader(index: Int): String = s"""elif [ $$i -eq $index ]${newLine}then"""
}

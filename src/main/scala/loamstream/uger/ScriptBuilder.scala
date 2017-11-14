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
  private val space: String = " "
  private val tab: String = "\t"
  private val newLine: String = "\n"
  private val unixLineSep: String = " \\"

  //NB: We need to 'use' Java-1.8 to make some steps of the QC pipeline work.  
  private val scriptHeader: String =
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
  private val endIf: String = s"""${newLine}fi$newLine"""

  def jobsWithUgerIndices(commandLineJobs: Seq[CommandLineJob]): Seq[(CommandLineJob, Int)] = {
    //Uger task array indices start from 1
    commandLineJobs.zipWithIndex.map { case (j, i) => (j, i + 1) } 
  }
  
  def buildFrom(commandLineJobs: Seq[CommandLineJob]): String = {
    val jobsAndIndices = jobsWithUgerIndices(commandLineJobs)
    
    val (firstJob, firstIndex) = jobsAndIndices.head
    
    val firstIfBlock = getFirstIfBlock(firstJob, firstIndex)

    val elseIfBlocks = jobsAndIndices.tail.map { case (job, index) =>
      s"${getElseIfHeader(index)}${getBody(job)}"
    }.mkString(newLine)

    s"$scriptHeader$newLine$firstIfBlock$newLine$elseIfBlocks$endIf"
  }

  private def getFirstIfBlock(commandLineJob: CommandLineJob, indexStartValue: Int): String = {
    val ifHeader = getIfHeader(indexStartValue)
    val ifBody = getBody(commandLineJob)

    s"$ifHeader$ifBody"
  }

  private def getBody(commandLineJob: CommandLineJob): String = {
    s"""$newLine$tab${commandLineJob.commandLineString}"""
  }

  private def getIfHeader(index: Int): String = s"""if [ $$i -eq $index ]${newLine}then"""

  private def getElseIfHeader(index: Int): String = s"""elif [ $$i -eq $index ]${newLine}then"""
}

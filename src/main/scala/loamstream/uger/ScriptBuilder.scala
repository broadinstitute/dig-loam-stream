package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob
import org.ggf.drmaa.JobTemplate

/**
  * @author Kaan
  *         Date: Jul 1, 2016
  *
  * Used to facilitate generation of bash scripts to submit task arrays to UGER
  * For an example of such scripts, see src/test/resources/imputation/shapeItUgerSubmissionScript.sh
  */
private[uger] object ScriptBuilder {
  final case class Params(
      preamble: Option[String], 
      indexEnvVarName: String, 
      jobIdEnvVarName: String,
      drmIndexVarExpr: String)
  
  object Params {
    //NB: We need to 'use' Java-1.8 to make some steps of the QC pipeline work.
    private val ugerPreamble="""|#$ -cwd
                                |
                                |source /broad/software/scripts/useuse
                                |reuse -q UGER
                                |reuse -q Java-1.8
                                |
                                |export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$PATH
                                |source activate loamstream_v1.0""".stripMargin
    
                                
    val uger = Params(
        preamble = Option(ugerPreamble), 
        indexEnvVarName = "SGE_TASK_ID", 
        jobIdEnvVarName = "JOB_ID", 
        drmIndexVarExpr = JobTemplate.PARAMETRIC_INDEX)
    
    val lsf = Params(
        preamble = None, 
        indexEnvVarName = "LSB_JOBINDEX", 
        jobIdEnvVarName = "LSB_JOBID",
        drmIndexVarExpr = "%I")
  }
  
  def uger: ScriptBuilder = new ScriptBuilder(Params.uger)
  def lsf: ScriptBuilder = new ScriptBuilder(Params.lsf)
}

private[uger] class ScriptBuilder(params: ScriptBuilder.Params) {
  private val space: String = " "
  private val newLine: String = "\n"
  private val unixLineSep: String = " \\"

  private val scriptHeader: String = {
    s"""|#!/bin/bash
        |${params.preamble.mkString}
        |
        |i=$$${params.indexEnvVarName}
        |jobId=$$${params.jobIdEnvVarName}
        |      """.stripMargin
  }
  
  private val endIf: String = s"${newLine}fi${newLine}"

  def buildFrom(taskArray: UgerTaskArray): String = {
    val ugerJobs = taskArray.ugerJobs
    
    val firstIfBlock = getFirstIfBlock(taskArray)

    val elseIfBlocks = ugerJobs.tail.map { ugerJob =>
      val index = ugerJob.drmIndex
      
      s"${getElseIfHeader(index)}${getBody(taskArray, ugerJob)}"
    }.mkString(newLine)

    s"${scriptHeader}${newLine}${firstIfBlock}${newLine}${elseIfBlocks}${endIf}"
  }

  private def getFirstIfBlock(taskArray: UgerTaskArray): String = {
    val ugerJob: UgerJobWrapper = taskArray.ugerJobs.head
    val indexStartValue: Int = ugerJob.drmIndex
    
    val ifHeader = getIfHeader(indexStartValue)
    val ifBody = getBody(taskArray, ugerJob)

    s"${ifHeader}${ifBody}"
  }

  private def getBody(taskArray: UgerTaskArray, ugerJob: UgerJobWrapper): String = {
    val commandChunk = ugerJob.ugerCommandChunk(taskArray)
    
    s"${newLine}${commandChunk}"
  }

  private def getIfHeader(index: Int): String = s"if [ $$i -eq $index ]${newLine}then"

  private def getElseIfHeader(index: Int): String = s"elif [ $$i -eq $index ]${newLine}then"
}

package loamstream.drm

import java.nio.file.Path

/**
  * @author Kaan
  *         clint
  *         Date: Jul 1, 2016
  *
  * Used to facilitate generation of bash scripts to submit task arrays to UGER
  * For an example of such scripts, see src/test/resources/imputation/shapeItUgerSubmissionScript.sh
  */
final class ScriptBuilder(params: ScriptBuilderParams) {
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

  def buildFrom(taskArray: DrmTaskArray): String = {
    val drmJobs = taskArray.drmJobs
    
    val firstIfBlock = getFirstIfBlock(taskArray)

    val elseIfBlocks = drmJobs.tail.map { ugerJob =>
      val index = ugerJob.drmIndex
      
      s"${getElseIfHeader(index)}${getBody(taskArray, ugerJob)}"
    }.mkString(newLine)

    s"${scriptHeader}${newLine}${firstIfBlock}${newLine}${elseIfBlocks}${endIf}"
  }

  private def getFirstIfBlock(taskArray: DrmTaskArray): String = {
    val drmJob: DrmJobWrapper = taskArray.drmJobs.head
    val indexStartValue: Int = drmJob.drmIndex
    
    val ifHeader = getIfHeader(indexStartValue)
    val ifBody = getBody(taskArray, drmJob)

    s"${ifHeader}${ifBody}"
  }

  private def getBody(taskArray: DrmTaskArray, drmJob: DrmJobWrapper): String = {
    val commandChunk = drmJob.commandChunk(taskArray)
    
    s"${newLine}${commandChunk}"
  }

  private def getIfHeader(index: Int): String = s"""if [ "$$i" = "$index" ]${newLine}then"""

  private def getElseIfHeader(index: Int): String = s"""elif [ "$$i" = "$index" ]${newLine}then"""
}

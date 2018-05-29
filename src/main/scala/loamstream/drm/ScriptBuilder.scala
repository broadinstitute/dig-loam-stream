package loamstream.drm

import org.ggf.drmaa.JobTemplate
import java.nio.file.Path
import java.nio.file.Paths
import loamstream.conf.DrmConfig
import loamstream.util.BashScript
import loamstream.drm.uger.UgerScriptBuilderParams

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
    val ugerJobs = taskArray.drmJobs
    
    val firstIfBlock = getFirstIfBlock(taskArray)

    val elseIfBlocks = ugerJobs.tail.map { ugerJob =>
      val index = ugerJob.drmIndex
      
      s"${getElseIfHeader(index)}${getBody(taskArray, ugerJob)}"
    }.mkString(newLine)

    s"${scriptHeader}${newLine}${firstIfBlock}${newLine}${elseIfBlocks}${endIf}"
  }

  private def getFirstIfBlock(taskArray: DrmTaskArray): String = {
    val ugerJob: DrmJobWrapper = taskArray.drmJobs.head
    val indexStartValue: Int = ugerJob.drmIndex
    
    val ifHeader = getIfHeader(indexStartValue)
    val ifBody = getBody(taskArray, ugerJob)

    s"${ifHeader}${ifBody}"
  }

  private def getBody(taskArray: DrmTaskArray, ugerJob: DrmJobWrapper): String = {
    val commandChunk = ugerJob.commandChunk(taskArray)
    
    s"${newLine}${commandChunk}"
  }

  private def getIfHeader(index: Int): String = s"if [ $$i -eq $index ]${newLine}then"

  private def getElseIfHeader(index: Int): String = s"elif [ $$i -eq $index ]${newLine}then"
}
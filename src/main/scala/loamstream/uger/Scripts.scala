package loamstream.uger

import loamstream.conf.ShapeItConfig
import java.nio.file.Path

/**
 * @author clint
 * date: Jun 17, 2016
 */
object Scripts {
  def forShapeit(config: ShapeItConfig, inputVcf: Path, outputHaps: Path, outputSamples: Path): String = {
    s"""#!/bin/bash
#$$ -cwd
#$$ -j y

${config.executable} \
-V $inputVcf \
-M ${config.mapFile} \
-O $outputHaps $outputSamples \
-L ${config.logFile} \
--thread ${numberOfCpuCores}
"""
  }
  
  private def numberOfCpuCores: Int = Runtime.getRuntime.availableProcessors
}
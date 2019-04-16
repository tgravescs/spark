/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io.File

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Discovers resources (GPUs/FPGAs/etc). Currently this just knows about gpus but
 * could easily be extended.
 */
private[spark] object ResourceDiscoverer extends Logging {

  private val GPU = "gpu"

  def findResources(sparkconf: SparkConf, isDriver: Boolean): Map[String, ResourceInformation] = {
    val gpus = getGPUResources(sparkconf, isDriver)
    if (gpus.isEmpty) {
      Map()
    } else {
      Map(GPU ->
        new ResourceInformation(GPU, "", gpus.size, gpus))
    }
  }

  private def getGPUResources(sparkconf: SparkConf, isDriver: Boolean): Array[String] = {
    val discoveryConf = if (isDriver) DRIVER_GPU_DISCOVERY_SCRIPT else EXECUTOR_GPU_DISCOVERY_SCRIPT
    val script = sparkconf.get(discoveryConf)
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        try {
          val output = executeAndGetOutput(Seq(script.get), new File("."))
          // sanity check output is a comma separate list of ints
          val gpu_ids = output.split(",").map(_.trim())
          for (gpu <- gpu_ids) {
            Integer.parseInt(gpu)
          }
          gpu_ids
        } catch {
          case e @ (_: SparkException | _: NumberFormatException) =>
            throw new SparkException(s"Error running the GPU discovery script: $scriptFile",
              e)
        }
      } else {
        throw new SparkException(s"GPU script: $scriptFile to discover GPUs doesn't exist!")
      }
    } else {
      logWarning(s"User is expecting to use GPU resources but didn't specify a " +
        s"script via conf: ${discoveryConf.key}, to find them!")
      Array.empty[String]
    }
    result
  }
}

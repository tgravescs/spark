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

package org.apache.spark.executor

import java.io.File

import scala.collection.mutable.Set

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.ResourceInformation
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Discover resources available based on user input, currently this just knows about gpus
 * but could easily be made more generic.
 */
private[spark] class ResourceDiscoverer(sparkconf: SparkConf) extends Logging {

   def findResources(): Array[String] = {
     val resourceTypes = "gpu"
     val allResourceInfo = Set[ResourceInformation]()

     if (sparkconf.get) {
       val types = resourceTypes.get.split(",")

       for (resourceType <- types) {
         val lookupType = SPARK_RESOURCE_TYPES_PREFIX + "." + resourceType
         val resourceConfigs = sparkconf.getAllWithPrefix(lookupType).toMap
         val unit = resourceConfigs.get("unit").getOrElse("")
         val resourceVal = resourceConfigs.get("value").getOrElse("0").toLong
         allResourceInfo += new ResourceInformation(resourceType, resourceVal, unit)
       }
     }

     return
   }

  def getGPUResources: Map[String, ResourceInformation] = {
    val resources = Map[String, ResourceInformation]
    val script = sparkconf.get(GPU_DISCOVERY_SCRIPT)
    if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        try {
          val output = executeAndGetOutput(Seq(script.get), new File("."))
          // sanity check output is a comma separate list of ints
          val gpu_ids = output.split(",")
          for (gpu <- gpu_ids) {
            Integer.parseInt(gpu.trim())
            resources + "gpu" -> new ResourceInformation("gpu", 0, "")
          }
          output
        } catch {
          case _: SparkException | _: NumberFormatException =>
            logError("The gpu discover script threw exception, assuming no gpu's", _)
            ""
        }
      }
    }
    resources
  }
}

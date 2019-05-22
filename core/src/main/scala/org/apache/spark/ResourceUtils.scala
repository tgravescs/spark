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

import java.io.{BufferedInputStream, File, FileInputStream}

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 *
 * @param componentName spark.driver / spark.executor / spark.task
 * @param resourceName  gpu, fpga
 */
private[spark] case class ResourceID(componentName: String, resourceName: String) {
  def confPrefix: String = s"$componentName.resource.$resourceName." // with ending dot
}

private[spark] case class ResourceRequest(
    id: ResourceID,
    count: Double,
    discoveryScript: Option[String])

private[spark] case class ResourceAllocation(id: ResourceID, addresses: Seq[String]) {
  def toResourceInfo(): ResourceInformation = {
    new ResourceInformation(id.resourceName, addresses.toArray)
  }
}

private[spark] object ResourceUtils extends Logging {

  def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest = {
    val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
    val quantity = settings.get(SPARK_RESOURCE_COUNT_SUFFIX).getOrElse(
      throw new SparkException("You must specify a count")).toDouble
    val discoveryScript = settings.get(SPARK_RESOURCE_DISCOVERY_SCRIPT_SUFFIX)
    ResourceRequest(resourceId, quantity, discoveryScript)
  }

  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.resource.").map { case (key, _) =>
      key.substring(0, key.indexOf('.'))
    }.toSet.toSeq.map(name => ResourceID(componentName, name))
  }

  def parseAllResourceRequests(
      sparkConf: SparkConf, componentName: String): Seq[ResourceRequest] = {
    listResourceIds(sparkConf, componentName).map { id =>
      parseResourceRequest(sparkConf, id)
    }
  }

  def parseAllocatedFromJsonFile(resourcesFile: String): Seq[ResourceAllocation] = {
    implicit val formats = DefaultFormats
    val resourceInput = new BufferedInputStream(new FileInputStream(resourcesFile))
    try {
      parse(resourceInput).extract[Seq[ResourceAllocation]]
    } catch {
      case e@(_: MappingException | _: MismatchedInputException | _: ClassCastException) =>
        throw new SparkException(s"Exception parsing the resources in $resourcesFile", e)
    } finally {
      resourceInput.close()
    }
  }

  def parseAllocatedAndDiscoverResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Seq[ResourceAllocation] = {
    val allocated = resourcesFileOpt.map(parseAllocatedFromJsonFile(_))
      .getOrElse(Seq.empty[ResourceAllocation])
      .filter(_.id.componentName == componentName)
    val otherResourceIds = listResourceIds(sparkConf, componentName).diff(allocated.map(_.id))
    allocated ++ otherResourceIds.map { id =>
      val request = parseResourceRequest(sparkConf, id)
      discoverResource(request)
    }
  }

  def assertResourceAllocationMeetsRequest(
      allocation: ResourceAllocation, request: ResourceRequest): Unit = {
    require(allocation.id == request.id && allocation.addresses.size >= request.count)
  }

  def assertAllResourceAllocationMeetRequests(
      allocations: Seq[ResourceAllocation], requests: Seq[ResourceRequest]): Unit = {
    val allocated = allocations.map(x => x.id -> x).toMap
    requests.foreach { r =>
      assertResourceAllocationMeetsRequest(allocated(r.id), r)
    }
  }

  def anyComponentResourceRequests(
      sparkConf: SparkConf,
      componentName: String): Boolean = {
    sparkConf.getAllWithPrefix(componentName).nonEmpty
  }

  // also add some methods to convert requests to Spark conf entries to simplify tests


  /**
   * This function will discover information about a set of resources by using the
   * user specified script (spark.{driver/executor}.resource.{resourceName}.discoveryScript).
   * It optionally takes a set of resource names or if that isn't specified
   * it uses the config prefix passed in to look at the executor or driver configs
   * to get the resource names. Then for each resource it will run the discovery script
   * and get the ResourceInformation about it.
   *
   * @param sparkConf SparkConf
   * @param confPrefix Driver or Executor resource prefix
   * @param resourceNamesOpt Optionally specify resource names. If not set uses the resource
   *                  configs based on confPrefix passed in to get the resource names.
   * @return Map of resource name to ResourceInformation
   */
  def discoverResource(resourceRequest: ResourceRequest): ResourceAllocation = ???

  def discoverResourcesInformation(
      sparkConf: SparkConf,
      confPrefix: String,
      resourceNamesOpt: Option[Set[String]] = None
      ): Map[String, ResourceInformation] = {
    val resourceNames = resourceNamesOpt.getOrElse(
      // get unique resource names by grabbing first part config with multiple periods,
      // ie resourceName.count, grab resourceName part
      SparkConf.getBaseOfConfigs(sparkConf.getAllWithPrefix(confPrefix))
    )
    resourceNames.map { rName => {
      val rInfo = getResourceInfo(sparkConf, confPrefix, rName)
      (rName -> rInfo)
    }}.toMap
  }

  private def getResourceInfo(
      sparkConf: SparkConf,
      confPrefix: String,
      resourceName: String): ResourceInformation = {
    val discoveryConf = confPrefix + resourceName + SPARK_RESOURCE_DISCOVERY_SCRIPT_SUFFIX
    val script = sparkConf.getOption(discoveryConf)
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        try {
          val output = executeAndGetOutput(Seq(script.get), new File("."))
          val parsedJson = parse(output)
          val name = (parsedJson \ "name").extract[String]
          val addresses = (parsedJson \ "addresses").extract[Array[String]]
          if (name != resourceName) {
            throw new SparkException(s"Discovery script: ${script.get} specified via " +
              s"$discoveryConf returned a resource name: $name that doesn't match the " +
              s"config name: $resourceName")
          }
          new ResourceInformation(name, addresses)
        } catch {
          case e @ (_: SparkException | _: MappingException | _: JsonParseException) =>
            throw new SparkException(s"Error running the resource discovery script: $scriptFile" +
              s" for $resourceName", e)
        }
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName" +
          s" doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use $resourceName resources but " +
        s"didn't specify a script via conf: $discoveryConf, to find them!")
    }
    result
  }

  /**
   * Make sure the actual resources we have on startup are at least the number the user
   * requested. Note that there is other code in SparkConf that makes sure we have executor configs
   * for each task resource requirement and that they are large enough. This function
   * is used by both driver and executors.
   *
   * @param requiredResources The resources that are required for us to run.
   * @param actualResources The actual resources discovered.
   */
  def checkActualResourcesMeetRequirements(
      requiredResources: Map[String, String],
      actualResources: Map[String, ResourceInformation]): Unit = {
    requiredResources.foreach { case (rName, reqCount) =>
      val actualRInfo = actualResources.get(rName).getOrElse(
        throw new SparkException(s"Resource: $rName required but wasn't discovered on startup"))

      if (actualRInfo.addresses.size < reqCount.toLong) {
        throw new SparkException(s"Resource: $rName, with addresses: " +
          s"${actualRInfo.addresses.mkString(",")} " +
          s"is less than what the user requested: $reqCount)")
      }
    }
  }
}

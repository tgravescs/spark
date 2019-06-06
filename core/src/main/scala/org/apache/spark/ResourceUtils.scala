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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JavaFiles}
import java.nio.file.attribute.PosixFilePermission.{OWNER_EXECUTE, OWNER_READ, OWNER_WRITE}
import java.util.EnumSet

import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.google.common.io.Files
import org.json4s.{DefaultFormats, MappingException}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods._

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * Resource identifier.
 * @param componentName spark.driver / spark.executor / spark.task
 * @param resourceName  gpu, fpga, etc
 */
private[spark] case class ResourceID(componentName: String, resourceName: String) {
  def confPrefix: String = s"$componentName.resource.$resourceName." // with ending dot
}

private[spark] case class ResourceRequest(
    id: ResourceID,
    count: Double,  // count or amount?? TODO
    discoveryScript: Option[String],
    vendor: Option[String]) {
}

private[spark] case class ResourceAllocation(id: ResourceID, addresses: Seq[String]) {
  def toResourceInfo(): ResourceInformation = {
    new ResourceInformation(id.resourceName, addresses.toArray)
  }
}

private[spark] object ResourceUtils extends Logging {

  val AMOUNT = ".amount"
  val DISCOVERY_SCRIPT = ".discoveryScript"
  val VENDOR = ".vendor"

  // case class to make extracting the JSON resource information easy
  case class JsonResourceInformation(name: String, addresses: Seq[String])

  def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest = {
    val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
    val quantity = settings.get(AMOUNT).getOrElse(
      throw new SparkException("You must specify an amount")).toDouble
    val discoveryScript = settings.get(DISCOVERY_SCRIPT)
    val vendor = settings.get(VENDOR)
    ResourceRequest(resourceId, quantity, discoveryScript, vendor)
  }

  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.resource.").map { case (key, _) =>
      key.substring(0, key.indexOf('.'))
    }.toSet.toSeq.map(name => ResourceID(componentName, name))
  }

  def parseAllResourceRequests(
      sparkConf: SparkConf,
      componentName: String): Seq[ResourceRequest] = {
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

  def parseResourceAllocationFromJson(resourcesJson: String): JsonResourceInformation = {
    implicit val formats = DefaultFormats
    try {
      parse(resourcesJson).extract[JsonResourceInformation]
    } catch {
      case e@(_: MappingException | _: MismatchedInputException | _: ClassCastException) =>
        throw new SparkException(s"Exception parsing the resources in $resourcesJson", e)
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
      ResourceAllocation(id, discoverResource(request).addresses)
    }
  }

  def assertResourceAllocationMeetsRequest(
      allocation: ResourceAllocation, request: ResourceRequest): Unit = {
    // TODO - test printing error message
    require(allocation.id == request.id && allocation.addresses.size >= request.count,
      s"Resource: ${allocation.id}, with addresses: " +
      s"${allocation.addresses.mkString(",")} " +
      s"is less than what the user requested: ${request.count})")
  }

  def assertAllResourceAllocationMeetRequests(
      allocations: Seq[ResourceAllocation], requests: Seq[ResourceRequest]): Unit = {
    val allocated = allocations.map(x => x.id -> x).toMap
    requests.foreach { r =>
      assertResourceAllocationMeetsRequest(allocated(r.id), r)
    }
  }

  def hasTaskComponentResourceRequests(sparkConf: SparkConf): Boolean = {
    sparkConf.getAllWithPrefix(SPARK_TASK_RESOURCE_PREFIX).nonEmpty
  }

  def getAllResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    val requests = parseAllResourceRequests(sparkConf, componentName)
    val allocations =
      parseAllocatedAndDiscoverResources(
        sparkConf,
        componentName,
        resourcesFileOpt)
    assertAllResourceAllocationMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInfo())).toMap
    logInfo("==============================================================")
    logInfo("Resources:")
    resourceInfoMap.foreach { case (k, v) => logInfo(s"$k -> $v") }
    logInfo("==============================================================")
    resourceInfoMap
  }

  def discoverResource(resourceRequest: ResourceRequest): JsonResourceInformation = {
    val resourceName = resourceRequest.id
    val script = resourceRequest.discoveryScript
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        val output = executeAndGetOutput(Seq(script.get), new File("."))
        parseResourceAllocationFromJson(output)
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
          "doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use resource: $resourceName but " +
        "didn't specify a discovery script!")
    }
    result
  }

  /**
   * Get task resource requirements.
   */
  def getTaskResourceRequirements(sparkConf: SparkConf): Map[String, Int] = {
    sparkConf.getAllWithPrefix(SPARK_TASK_RESOURCE_PREFIX)
      .withFilter { case (k, v) => k.endsWith(AMOUNT)}
      .map { case (k, v) => (k.dropRight(AMOUNT.length), v.toInt)}.toMap
  }

  def resourceAmountConfigName(id: ResourceID): String = {
    s"${id.componentName}${id.resourceName}$AMOUNT"
  }

  def resourceDiscoveryScriptConfigName(id: ResourceID): String = {
    s"${id.componentName}${id.resourceName}$DISCOVERY_SCRIPT"
  }

  def resourceVendorConfigName(id: ResourceID): String = {
    s"${id.componentName}${id.resourceName}$VENDOR"
  }

  def setDriverResourceAmountConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_DRIVER_RESOURCE_PREFIX, resourceName)
    setResourceAmountConf(conf, resourceId, value)
  }

  def setDriverResourceDiscoveryConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_DRIVER_RESOURCE_PREFIX, resourceName)
    setResourceDiscoveryScriptConf(conf, resourceId, value)
  }

  def setExecutorResourceAmountConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_EXECUTOR_RESOURCE_PREFIX, resourceName)
    setResourceAmountConf(conf, resourceId, value)
  }

  def setExecutorResourceDiscoveryConf(
      conf: SparkConf,
      resourceName: String,
      value: String): Unit = {
    val resourceId = ResourceID(SPARK_EXECUTOR_RESOURCE_PREFIX, resourceName)
    setResourceDiscoveryScriptConf(conf, resourceId, value)
  }

  def setExecutorResourceVendorConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_EXECUTOR_RESOURCE_PREFIX, resourceName)
    setResourceVendorConf(conf, resourceId, value)
  }

  def setTaskResourceAmountConf(conf: SparkConf, resourceName: String, value: String): Unit = {
    val resourceId = ResourceID(SPARK_TASK_RESOURCE_PREFIX, resourceName)
    setResourceAmountConf(conf, resourceId, value)
  }

  def setResourceAmountConf(conf: SparkConf, id: ResourceID, value: String) {
    conf.set(resourceAmountConfigName(id), value)
  }

  def setResourceDiscoveryScriptConf(conf: SparkConf, id: ResourceID, value: String) {
    conf.set(resourceAmountConfigName(id), value)
  }

  def setResourceVendorConf(conf: SparkConf, id: ResourceID, value: String) {
    conf.set(resourceVendorConfigName(id), value)
  }

  // known types of resources
  final val GPU: String = "gpu"
  final val FPGA: String = "fpga"

  // TESTING UTILS

  def writeJsonFile(dir: File, strToWrite: JArray): String = {
    val f1 = File.createTempFile("jsonResourceFile", "", dir)
    JavaFiles.write(f1.toPath(), compact(render(strToWrite)).getBytes())
    f1.getPath()
  }

  def mockDiscoveryScript(file: File, result: String): String = {
    Files.write(s"echo $result", file, StandardCharsets.UTF_8)
    JavaFiles.setPosixFilePermissions(file.toPath(),
      EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
    file.getPath()
  }
}

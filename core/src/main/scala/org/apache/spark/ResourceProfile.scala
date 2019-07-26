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

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.immutable.HashMap
import scala.collection.mutable

import org.apache.spark.annotation.Evolving
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource._


/**
 * Class to hold information about the resources required when this rdd
 * is executed.
 *
 * @param name the name of the resource
 * @param units the units of the resources, can be an empty string if units don't apply
 * @param count the number of resources available
 * @param addresses an optional array of strings describing the addresses of the resource
 */
@Evolving
class ResourceProfile(taskReqs: Map[String, ResourceRequest]) extends Serializable with Logging {


  // TODO - want to add both task requirements and executor requirements for above
  // Perhaps use sparkConf as a way??? - just for the internal implementation,
  // I think for user focus we want a clear definition of what we support

  // executor:
  //  memory - heap, offheap(this is taken from overhead), overhead, pyspark, fraction
  //  cores
  //  resources - GPU, FPGA/etc
  //  number of executors? - if dynamic allocation not on - min/max number

  // Task requirements:
  //   memory -
  //   cores -
  //   resources -
  //   # of tasks for this stage? - not to start with

  private val id = ResourceProfile.getNextProfileId

  def getId: Int = id

  logInfo("resource profile has id: " + getId)

  private val taskResources: mutable.Map[String, ResourceRequest] =
    mutable.Map.empty[String, ResourceRequest]

  // TODO - do we need ExecutorResourceProfile separate to track allocations, etc.
  private val executorResources: mutable.Map[String, ResourceRequest] =
    mutable.Map.empty[String, ResourceRequest]

  private val allowedExecutor = HashMap[String, Boolean](
    ("memory" -> true),
    ("cores" -> true),
    ("memoryOverhead" -> true), // yarn only?
    ("pyspark.memory" -> true),
    ("gpu" -> true), // TODO - how to do generic resource types??
    ("instances" -> true))

  private val allowedTask = HashMap[String, Boolean](
    ("cpus" -> true),
    ("resource" -> true))

  private val allowedComponents = HashMap(
    (SPARK_EXECUTOR_PREFIX -> allowedExecutor),
    (SPARK_TASK_PREFIX -> allowedTask)
  )

  private val resourcesMap = HashMap(
    (SPARK_EXECUTOR_PREFIX -> executorResources),
    (SPARK_TASK_PREFIX -> taskResources)
  )

  // TODO - allow other spark confs?
  // TODO - allow DYN_ALLOCATION_EXECUTOR_ALLOCATION_RATIO?



  def getResources: Map[String, ResourceRequest] = taskResources.toMap

  def getTaskResources: Map[String, TaskResourceRequirement] = {
    taskResources.map { case (name, rr) =>
      (name, TaskResourceRequirement(name, rr.amount))
    }.toMap
  }

  // TODO - handle mapping resource.XXX to actual name
  def getExecutorResources: Map[String, ExecutorResourceRequirement] = {
    executorResources.map { case (name, rr) =>
      (name, ExecutorResourceRequirement(name, rr.amount))
    }.toMap
  }

  def require(sparkConf: SparkConf): Unit = {
    // check and only allow certain ones
  }

  private def allowedResource(rid: ResourceID): Boolean = {
    (allowedComponents.contains(rid.componentName) &&
      allowedComponents(rid.componentName).contains(rid.resourceName))
  }

  private def addResourceRequest(request: ResourceRequest): Unit = {
    resourcesMap(request.id.componentName)(request.id.resourceName) = request
  }

  def require(request: ResourceRequest): this.type = {
    if (allowedResource(request.id)) {
      addResourceRequest(request)
    } else {
      throw new IllegalArgumentException(s"Resource id not allowed: ${request.id}")
    }
    this
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ResourceProfile =>
        that.getClass == this.getClass &&
          that.taskResources == taskResources && that.executorResources == executorResources
      case _ =>
        false
    }
  }

  override def hashCode(): Int = Seq(taskResources, executorResources).hashCode()

  override def toString(): String = {
    s"Profile: id = $id, executor resources: $executorResources, task resources: $taskResources"
  }


  // def prefer(name: String, value: Long, units: String): Unit = {
  // }
}

private[spark] object ResourceProfile {
  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0
  private val defaultProfileRef: AtomicReference[ResourceProfile] =
    new AtomicReference[ResourceProfile](null)

  private val nextProfileId = new AtomicInteger(0)

  def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    synchronized {
      if (defaultProfileRef.get() == null) {
        val rp = new ResourceProfile(Map.empty).
          require(ResourceRequest(ResourceID(SPARK_EXECUTOR_PREFIX, "cores"),
            conf.get(EXECUTOR_CORES), None, None)).
          require(ResourceRequest(ResourceID(SPARK_EXECUTOR_PREFIX, "memory"),
            conf.get(EXECUTOR_MEMORY).toInt, None, None))
        assert(rp.getId == DEFAULT_RESOURCE_PROFILE_ID,
          s"Default Profile must have the default profile id: $DEFAULT_RESOURCE_PROFILE_ID")
        defaultProfileRef.set(rp)
        rp
      } else {
        defaultProfileRef.get()
      }
    }
  }

  def getNextProfileId: Int = nextProfileId.getAndIncrement()

}

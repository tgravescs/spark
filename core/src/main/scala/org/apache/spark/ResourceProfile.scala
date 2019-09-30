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
 * Resource profile to associate with an RDD.
 */
@Evolving
class ResourceProfile(taskReqs: Map[String, TaskResourceRequest])
  extends Serializable with Logging {

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

  logInfo(s"ResourceProfile has id: $id")

  private val taskResources = new mutable.HashMap[String, TaskResourceRequest]()
  // ++= ResourceProfile.getOrCreateDefaultProfile(sparkConf).getTaskResources
  taskResources ++= taskReqs

  // TODO - do we really want to add all the default profile values - might not want extra
  // resources !!!
  private val executorResources = new mutable.HashMap[String, ExecutorResourceRequest]()
  // ++= ResourceProfile.getOrCreateDefaultProfile(sparkConf).


  private val allowedExecutor = HashMap[String, Boolean](
    ("memory" -> true),
    ("cores" -> true),
    ("memoryOverhead" -> true), // yarn only?
    ("pyspark.memory" -> true),
    ("gpu" -> true), // TODO - how to do generic resource types??
    ("instances" -> true))

  // TODO - handle mapping resource.XXX to actual name


  private val allowedTask = HashMap[String, Boolean](
    ("cpus" -> true),
    ("resource" -> true))

  // merge this profile with another and return the result
  // TODO - handle both task and executor resources
  def merge(other: ResourceProfile): ResourceProfile = {
    throw new UnsupportedOperationException("merge not yet supported")
  }
  /*
  def merge(other: ResourceProfile): ResourceProfile = {
    val mergedKeys = this.getResources.keySet ++ other.getResources.keySet
    val merged = mergedKeys.map { rName =>
      if (other.getResources.contains(rName) && this.getResources.contains(rName)) {
        val r2ri = other.getResources(rName)
        val r1ri = this.getResources(rName)
        // TODO - do we want to sum things like Memory?
        if (r2ri.amount > r1ri.amount) (rName, r2ri) else (rName, r1ri)
      } else if (other.getResources.contains(rName)) {
        (rName, other.getResources(rName))
      } else {
        (rName, this.getResources(rName))
      }
    }.toMap
    new ResourceProfile(merged)
  }
  */

  def getTaskResources: Map[String, TaskResourceRequest] = taskResources.toMap

  def getExecutorResources: Map[String, ExecutorResourceRequest] = executorResources.toMap

  private def addResourceRequest(request: TaskResourceRequest): Unit = {
    taskResources(request.resourceName) = request
  }

  private def addResourceRequest(request: ExecutorResourceRequest): Unit = {
    executorResources(request.resourceName) = request
  }

  def require(request: TaskResourceRequest): this.type = {
   if (allowedTask(request.resourceName)) {
      addResourceRequest(request)
    } else {
      throw new IllegalArgumentException(s"Resource not allowed: ${request.resourceName}")
    }
    this
  }

  def require(request: ExecutorResourceRequest): this.type = {
    if (allowedExecutor(request.resourceName)) {
      addResourceRequest(request)
    } else {
      throw new IllegalArgumentException(s"Resource not allowed: ${request.resourceName}")
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
}

private[spark] object ResourceProfile {
  val UNKNOWN_RESOURCE_PROFILE_ID = -1
  val DEFAULT_RESOURCE_PROFILE_ID = 0

  // create default profile immediately to get ID 0, its initialized later when fetched
  private val defaultProfileRef: AtomicReference[ResourceProfile] =
    new AtomicReference[ResourceProfile](new ResourceProfile(Map.empty))

  assert(defaultProfileRef.get().getId == DEFAULT_RESOURCE_PROFILE_ID,
    s"Default Profile must have the default profile id: $DEFAULT_RESOURCE_PROFILE_ID")

  private val nextProfileId = new AtomicInteger(0)

  def getOrCreateDefaultProfile(conf: SparkConf): ResourceProfile = {
    val defaultProf = defaultProfileRef.get()
    // check to see if the default profile was initialized yet
    if (defaultProf.getExecutorResources == Map.empty) {
      synchronized {
        val prof = defaultProfileRef.get()
        if (prof.getExecutorResources == Map.empty) {
          val rp = prof.
            require(new ExecutorResourceRequest("cores",
              conf.get(EXECUTOR_CORES), None, None)).
            require(new ExecutorResourceRequest("memory",
              conf.get(EXECUTOR_MEMORY).toInt, None, None))
          // TODO - NEED TO add any resources
          defaultProfileRef.compareAndSet(null, rp)
          rp
        } else {
          defaultProfileRef.get()
        }
      }
    } else {
      defaultProf
    }
  }

  def getNextProfileId: Int = nextProfileId.getAndIncrement()

}

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
package org.apache.spark.scheduler.cluster.k8s

import java.util.Locale

import io.fabric8.kubernetes.api.model.ContainerStateTerminated
import io.fabric8.kubernetes.api.model.Pod
import scala.collection.mutable

import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.Logging

/**
 * An immutable view of the current executor pods that are running in the cluster.
 */
private[spark]
case class ExecutorPodsSnapshot(executorPods: Map[Int, Map[Long, ExecutorPodState]]) {

  import ExecutorPodsSnapshot._

  def withUpdate(updatedPod: Pod): ExecutorPodsSnapshot = {
    val rpId = updatedPod.getMetadata.getLabels.get(SPARK_RESOURCE_PROFILE_ID_LABEL).toInt
    val execIdToStates = toStatesByExecutorIdOrig(Seq(updatedPod))
    val newExecutorPods = Map(rpId -> (executorPods.getOrElse(rpId, Map.empty) ++ execIdToStates))
    new ExecutorPodsSnapshot(executorPods ++ newExecutorPods)
  }
}

object ExecutorPodsSnapshot extends Logging {
  private var shouldCheckAllContainers: Boolean = _

  def apply(executorPods: Seq[Pod]): ExecutorPodsSnapshot = {
    ExecutorPodsSnapshot(toStatesByExecutorId(executorPods))
  }

  def apply(): ExecutorPodsSnapshot =
    ExecutorPodsSnapshot(Map[Int, Map[Long, ExecutorPodState]]())

  def setShouldCheckAllContainers(watchAllContainers: Boolean): Unit = {
    shouldCheckAllContainers = watchAllContainers
  }

  private def toStatesByExecutorIdOrig(executorPods: Seq[Pod]): Map[Long, ExecutorPodState] = {
    executorPods.map { pod =>
      (pod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL).toLong, toState(pod))
    }.toMap
  }

  // TODO - rename
  private def toStatesByExecutorId(
      executorPods: Seq[Pod]): Map[Int, Map[Long, ExecutorPodState]] = {
    val rpIdToStateMap = new mutable.HashMap[Int, mutable.LinkedHashMap[Long, ExecutorPodState]]()
    val idToPod = executorPods.map { pod =>
      val rpId = pod.getMetadata.getLabels.get(SPARK_RESOURCE_PROFILE_ID_LABEL).toInt
      val execIdToState =
        rpIdToStateMap.getOrElseUpdate(rpId, mutable.LinkedHashMap[Long, ExecutorPodState]())
      execIdToState(pod.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL).toLong) = toState(pod)
    }
    rpIdToStateMap.map { case (rpId, execMap) =>
      (rpId, execMap.toMap)
    }.toMap
  }

  private def toState(pod: Pod): ExecutorPodState = {
    if (isDeleted(pod)) {
      PodDeleted(pod)
    } else {
      val phase = pod.getStatus.getPhase.toLowerCase(Locale.ROOT)
      phase match {
        case "pending" =>
          PodPending(pod)
        case "running" =>
          if (shouldCheckAllContainers &&
            "Never" == pod.getSpec.getRestartPolicy &&
            pod.getStatus.getContainerStatuses.stream
              .map[ContainerStateTerminated](cs => cs.getState.getTerminated)
              .anyMatch(t => t != null && t.getExitCode != 0)) {
            PodFailed(pod)
          } else {
            PodRunning(pod)
          }
        case "failed" =>
          PodFailed(pod)
        case "succeeded" =>
          PodSucceeded(pod)
        case "terminating" =>
          PodTerminating(pod)
        case _ =>
          logWarning(s"Received unknown phase $phase for executor pod with name" +
            s" ${pod.getMetadata.getName} in namespace ${pod.getMetadata.getNamespace}")
          PodUnknown(pod)
      }
    }
  }

  private def isDeleted(pod: Pod): Boolean = {
    (pod.getMetadata.getDeletionTimestamp != null &&
      (
        pod.getStatus == null ||
        pod.getStatus.getPhase == null ||
        pod.getStatus.getPhase.toLowerCase(Locale.ROOT) != "terminating"
      ))
  }
}

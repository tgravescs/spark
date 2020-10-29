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

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable
import scala.util.control.NonFatal

import io.fabric8.kubernetes.api.model.{PersistentVolumeClaim, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient

import org.apache.spark.{SecurityManager, SparkConf, SparkException}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesConf
import org.apache.spark.deploy.k8s.KubernetesUtils.addOwnerReference
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.SchedulerBackendUtils
import org.apache.spark.util.{Clock, Utils}

private[spark] class ExecutorPodsAllocator(
    conf: SparkConf,
    secMgr: SecurityManager,
    executorBuilder: KubernetesExecutorBuilder,
    kubernetesClient: KubernetesClient,
    snapshotsStore: ExecutorPodsSnapshotsStore,
    clock: Clock) extends Logging {

  private val EXECUTOR_ID_COUNTER = new AtomicLong(0L)

  // ResourceProfile id -> total expected executors per profile, currently we don't remove
  // any resource profiles - https://issues.apache.org/jira/browse/SPARK-30749
  private val totalExpectedExecutorsPerResourceProfileId = new mutable.LinkedHashMap[Int, Int]

  private val rpIdToResourceProfile = new mutable.HashMap[Int, ResourceProfile]

  private val podAllocationSize = conf.get(KUBERNETES_ALLOCATION_BATCH_SIZE)

  private val podAllocationDelay = conf.get(KUBERNETES_ALLOCATION_BATCH_DELAY)

  private val podCreationTimeout = math.max(podAllocationDelay * 5, 60000)

  private val executorIdleTimeout = conf.get(DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT) * 1000

  private val namespace = conf.get(KUBERNETES_NAMESPACE)

  private val kubernetesDriverPodName = conf
    .get(KUBERNETES_DRIVER_POD_NAME)

  private val shouldDeleteExecutors = conf.get(KUBERNETES_DELETE_EXECUTORS)

  private val driverPod = kubernetesDriverPodName
    .map(name => Option(kubernetesClient.pods()
      .withName(name)
      .get())
      .getOrElse(throw new SparkException(
        s"No pod was found named $kubernetesDriverPodName in the cluster in the " +
          s"namespace $namespace (this was supposed to be the driver pod.).")))

  // ResourceProfile ID to Executor IDs where
  // Executor IDs that have been requested from Kubernetes but have not been detected in any
  // snapshot yet. Mapped to the timestamp when they were created.
  // private val newlyCreatedExecutors = mutable.LinkedHashMap.empty[Long, Long]
  // private val newlyCreatedExecutors = mutable.HashMap[Int, mutable.LinkedHashMap[Long, Long]]()
  private val newlyCreatedExecutors = mutable.LinkedHashMap[Long, (Int, Long)]()


  private val dynamicAllocationEnabled = Utils.isDynamicAllocationEnabled(conf)

  private val hasPendingPods = new AtomicBoolean()

  private var lastSnapshot = ExecutorPodsSnapshot(Nil)

  // Executors that have been deleted by this allocator but not yet detected as deleted in
  // a snapshot from the API server. This is used to deny registration from these executors
  // if they happen to come up before the deletion takes effect.
  @volatile private var deletedExecutorIds = Set.empty[Long]

  def start(applicationId: String): Unit = {
    snapshotsStore.addSubscriber(podAllocationDelay) {
      onNewSnapshots(applicationId, _)
    }
  }

  private def getOrUpdateTotalNumExecutorsForRPId(rpId: Int): Int = synchronized {
    totalExpectedExecutorsPerResourceProfileId.getOrElseUpdate(rpId,
      SchedulerBackendUtils.getInitialTargetExecutorNumber(conf))
  }

  def setTotalExpectedExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Unit = synchronized {
    resourceProfileToTotalExecs.foreach { case (rp, numExecs) =>
      rpIdToResourceProfile.getOrElseUpdate(rp.id, rp)
      if (numExecs != getOrUpdateTotalNumExecutorsForRPId(rp.id)) {
        logInfo(s"Driver requested a total number of $numExecs executor(s) " +
          s"for resource profile id: ${rp.id}.")
        totalExpectedExecutorsPerResourceProfileId(rp.id) = numExecs
      }
    }
    if (!hasPendingPods.get()) {
      snapshotsStore.notifySubscribers()
    }
  }

  def isDeleted(executorId: String): Boolean = deletedExecutorIds.contains(executorId.toLong)

  private def onNewSnapshots(
      applicationId: String,
      snapshots: Seq[ExecutorPodsSnapshot]): Unit = synchronized {

    newlyCreatedExecutors --= snapshots.flatMap(_.executorPods.keys)

    // For all executors we've created against the API but have not seen in a snapshot
    // yet - check the current time. If the current time has exceeded some threshold,
    // assume that the pod was either never created (the API server never properly
    // handled the creation request), or the API server created the pod but we missed
    // both the creation and deletion events. In either case, delete the missing pod
    // if possible, and mark such a pod to be rescheduled below.
    val currentTime = clock.getTimeMillis()
    val timedOut = newlyCreatedExecutors.flatMap { case (execId, (rpId, timeCreated)) =>
      if (currentTime - timeCreated > podCreationTimeout) {
        Some(execId)
      } else {
        logDebug(s"Executor with id $execId was not found in the Kubernetes cluster since it" +
          s" was created ${currentTime - timeCreated} milliseconds ago.")
        None
      }
    }

    if (timedOut.nonEmpty) {
      logWarning(s"Executors with ids ${timedOut.mkString(",")} were not detected in the" +
        s" Kubernetes cluster after $podCreationTimeout ms despite the fact that a previous" +
        " allocation attempt tried to create them. The executors may have been deleted but the" +
        " application missed the deletion event.")

      newlyCreatedExecutors --= timedOut
      if (shouldDeleteExecutors) {
        Utils.tryLogNonFatalError {
          kubernetesClient
            .pods()
            .withLabel(SPARK_APP_ID_LABEL, applicationId)
            .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
            .withLabelIn(SPARK_EXECUTOR_ID_LABEL, timedOut.toSeq.map(_.toString): _*)
            .delete()
        }
      }
    }

    if (snapshots.nonEmpty) {
      lastSnapshot = snapshots.last
    }

    // Make a local, non-volatile copy of the reference since it's used multiple times. This
    // is the only method that modifies the list, so this is safe.
    var _deletedExecutorIds = deletedExecutorIds
    if (snapshots.nonEmpty) {
      val existingExecs = lastSnapshot.executorPods.keySet
      _deletedExecutorIds = _deletedExecutorIds.filter(existingExecs.contains)
    }

    logWarning("newly createdi is: " + newlyCreatedExecutors)

    // map the pods into per ResourceProfile id so we can check per ResourceProfile
    // fast path if not using other ResourceProfiles
    val rpIdToExecsAndPodState = mutable.HashMap[Int, mutable.LinkedHashMap[Long, ExecutorPodState]]()
    if (totalExpectedExecutorsPerResourceProfileId.size <= 1) {
      rpIdToExecsAndPodState(ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID) =
        mutable.LinkedHashMap.empty ++= lastSnapshot.executorPods
    } else {
      lastSnapshot.executorPods.foreach { case (execId, execPodState) =>
        val rpId = execPodState.pod.getMetadata.getLabels.get(SPARK_RESOURCE_PROFILE_ID_LABEL).toInt
        val execPods = rpIdToExecsAndPodState.getOrElseUpdate(rpId,
          mutable.LinkedHashMap[Long, ExecutorPodState]())
        execPods(execId) = execPodState
      }
    }

    var knownPendingCount = 0
    var totalRunningCount = 0
    // This is going to request the executors in order the resource profiles were
    // added. We aren't using any priorities based on the resource profiles.
    totalExpectedExecutorsPerResourceProfileId.foreach { case (rpId, targetNum) =>
      val snapshotsForRpId = rpIdToExecsAndPodState.getOrElse(rpId, mutable.LinkedHashMap.empty)

      val currentRunningCount = snapshotsForRpId.values.count {
        case PodRunning(_) => true
        case _ => false
      }
      totalRunningCount += currentRunningCount

      val currentPendingExecutors = snapshotsForRpId.filter {
        case (_, PodPending(_)) => true
        case _ => false
      }

      // its expected newlyCreatedExecutors should be small since we only allocate in small
      // batches - podAllocationSize
      logWarning("getting newly created for rpid $rpId")
      val newlyCreatedExecutorsForRpId =
      newlyCreatedExecutors.filter { case (execid, (waitingRpId, _)) =>
        logWarning(s"new ecreted filter $rpId checking: $waitingRpId")
        rpId == waitingRpId
      }

      if (snapshotsForRpId.nonEmpty) {
        logDebug(s"Pod for ResourceProfile Id: $rpId " +
          s"allocation status: $currentRunningCount running, " +
          s"${currentPendingExecutors.size} pending. " +
          s"${newlyCreatedExecutorsForRpId.size} unacknowledged.")
      }

      // This variable is used later to print some debug logs. It's updated when cleaning up
      // excess pod requests, since currentPendingExecutors is immutable.
      knownPendingCount += currentPendingExecutors.size

      // It's possible that we have outstanding pods that are outdated when dynamic allocation
      // decides to downscale the application. So check if we can release any pending pods early
      // instead of waiting for them to time out. Drop them first from the unacknowledged list,
      // then from the pending. However, in order to prevent too frequent fluctuation, newly
      // requested pods are protected during executorIdleTimeout period.
      //
      // TODO: with dynamic allocation off, handle edge cases if we end up with more running
      // executors than expected.
      logWarning(s"rp: $rpId running: $currentRunningCount pending: " +
        s"${currentPendingExecutors.size} newly created: ${newlyCreatedExecutorsForRpId.size}")
      val knownPodCount = currentRunningCount + currentPendingExecutors.size +
        newlyCreatedExecutorsForRpId.size

      if (knownPodCount > targetNum) {
        val excess = knownPodCount - targetNum
        val knownPendingToDelete = currentPendingExecutors
          .filter(x => isExecutorIdleTimedOut(x._2, currentTime))
          .map { case (id, _) => id }
          .take(excess - newlyCreatedExecutorsForRpId.size)
        val toDelete = newlyCreatedExecutorsForRpId
          .filter { case (_, (_, createTime)) =>
            currentTime - createTime > executorIdleTimeout
          }.keys.take(excess).toList ++ knownPendingToDelete

        if (toDelete.nonEmpty) {
          logInfo(s"Deleting ${toDelete.size} excess pod requests (${toDelete.mkString(",")}).")
          _deletedExecutorIds = _deletedExecutorIds ++ toDelete

          Utils.tryLogNonFatalError {
            kubernetesClient
              .pods()
              .withField("status.phase", "Pending")
              .withLabel(SPARK_APP_ID_LABEL, applicationId)
              .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
              .withLabelIn(SPARK_EXECUTOR_ID_LABEL, toDelete.sorted.map(_.toString): _*)
              .delete()
            newlyCreatedExecutors --= toDelete
            knownPendingCount -= knownPendingToDelete.size
          }
        }
      }

      if (newlyCreatedExecutorsForRpId.isEmpty && knownPodCount < targetNum) {
        requestNewExecutors(targetNum, knownPodCount, applicationId, rpId)
      }
    }

    deletedExecutorIds = _deletedExecutorIds

    // Update the flag that helps the setTotalExpectedExecutors() callback avoid triggering this
    // update method when not needed.
    hasPendingPods.set(knownPendingCount + newlyCreatedExecutors.size > 0)

    // The code below just prints debug messages, which are only useful when there's a change
    // in the snapshot state. Since the messages are a little spammy, avoid them when we know
    // there are no useful updates.
    if (!log.isDebugEnabled || snapshots.isEmpty) {
      return
    }

    val totalExpected = totalExpectedExecutorsPerResourceProfileId.values.sum
    if (totalRunningCount >= totalExpected && !dynamicAllocationEnabled) {
      logDebug("Current number of running executors is equal to the number of requested" +
        " executors. Not scaling up further.")
    } else {
      val outstanding = knownPendingCount + newlyCreatedExecutors.size
      if (outstanding > 0) {
        logDebug(s"Still waiting for $outstanding executors before requesting more.")
      }
    }
  }

  private def requestNewExecutors(
      expected: Int, running: Int, applicationId: String, resourceProfileId: Int): Unit = {
    val numExecutorsToAllocate = math.min(expected - running, podAllocationSize)
    logInfo(s"Going to request $numExecutorsToAllocate executors from Kubernetes for " +
      s"ResourceProfile Id: $resourceProfileId, target: $expected running: $running.")
    for ( _ <- 0 until numExecutorsToAllocate) {
      val newExecutorId = EXECUTOR_ID_COUNTER.incrementAndGet()
      val executorConf = KubernetesConf.createExecutorConf(
        conf,
        newExecutorId.toString,
        applicationId,
        driverPod,
        resourceProfileId)
      val profile = rpIdToResourceProfile(resourceProfileId)
      logWarning(s"profile is $profile")
      val resolvedExecutorSpec = executorBuilder.buildFromFeatures(executorConf, secMgr,
        kubernetesClient, rpIdToResourceProfile(resourceProfileId))
      val executorPod = resolvedExecutorSpec.pod
      val podWithAttachedContainer = new PodBuilder(executorPod.pod)
        .editOrNewSpec()
        .addToContainers(executorPod.container)
        .endSpec()
        .build()
      val createdExecutorPod = kubernetesClient.pods().create(podWithAttachedContainer)
      try {
        val resources = resolvedExecutorSpec.executorKubernetesResources
        addOwnerReference(createdExecutorPod, resources)
        resources
          .filter(_.getKind == "PersistentVolumeClaim")
          .foreach { resource =>
            val pvc = resource.asInstanceOf[PersistentVolumeClaim]
            logInfo(s"Trying to create PersistentVolumeClaim ${pvc.getMetadata.getName} with " +
              s"StorageClass ${pvc.getSpec.getStorageClassName}")
            kubernetesClient.persistentVolumeClaims().create(pvc)
          }
        newlyCreatedExecutors(newExecutorId) = (resourceProfileId, clock.getTimeMillis())
        logDebug(s"Requested executor with id $newExecutorId from Kubernetes.")
      } catch {
        case NonFatal(e) =>
          kubernetesClient.pods().delete(createdExecutorPod)
          throw e
      }
    }
  }

  private def isExecutorIdleTimedOut(state: ExecutorPodState, currentTime: Long): Boolean = {
    try {
      val startTime = Instant.parse(state.pod.getStatus.getStartTime).toEpochMilli()
      currentTime - startTime > executorIdleTimeout
    } catch {
      case _: Exception =>
        logDebug(s"Cannot get startTime of pod ${state.pod}")
        true
    }
  }
}

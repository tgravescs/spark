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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files => JavaFiles}
import java.nio.file.attribute.PosixFilePermission._
import java.util.EnumSet

import com.google.common.io.Files

import org.apache.spark._
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils


class ResourceDiscovererSuite extends SparkFunSuite
    with LocalSparkContext {

  test("Resource discoverer no gpus") {
    val sparkconf = new SparkConf
    val resources = ResourceDiscoverer.findResources(sparkconf, false)
    assert(resources.get("gpu").isEmpty,
      "Should have a gpus entry that is empty")
  }

  test("Resource discoverer multiple gpus") {
    val sparkconf = new SparkConf

    assume(!(Utils.isWindows))

    withTempDir { dir =>
      val file1 = new File(dir, "resourceDiscoverScript1")
      Files.write("echo 0,1", file1, StandardCharsets.UTF_8)
      JavaFiles.setPosixFilePermissions(file1.toPath(),
        EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, file1.getPath())
      val resources = ResourceDiscoverer.findResources(sparkconf, false)
      val gpuValue = resources.get("gpu")
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.getCount() == 2, "Should have 2")
      assert(gpuValue.get.getName() == "gpu", "name should be gpu")
      assert(gpuValue.get.getUnits() == "", "units should be empty")
      assert(gpuValue.get.getAddresses().size == 2, "Should have 2 indexes")
      assert(gpuValue.get.getAddresses().deep == Array("0", "1").deep, "should have 0,1 entries")

    }
  }

  test("Resource discoverer multiple gpus driver") {
    val sparkconf = new SparkConf

    withTempDir { dir =>
      val file1 = new File(dir, "resourceDiscoverScript2")
      Files.write("echo 0,1", file1, StandardCharsets.UTF_8)
      JavaFiles.setPosixFilePermissions(file1.toPath(),
        EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
      sparkconf.set(SPARK_DRIVER_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, file1.getPath())
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, "boguspath")
      // make sure it reads from correct config, here it should use driver
      val resources = ResourceDiscoverer.findResources(sparkconf, true)
      val gpuValue = resources.get("gpu")
      assert(gpuValue.nonEmpty, "Should have a gpu entry")
      assert(gpuValue.get.getCount() == 2, "Should have 2")
      assert(gpuValue.get.getName() == "gpu", "name should be gpu")
      assert(gpuValue.get.getUnits() == "", "units should be empty")
      assert(gpuValue.get.getAddresses().size == 2, "Should have 2 indexes")
      assert(gpuValue.get.getAddresses().deep == Array("0", "1").deep, "should have 0,1 entries")

    }
  }

  test("Resource discoverer script returns invalid number") {
    val sparkconf = new SparkConf

    withTempDir { dir =>
      val file1 = new File(dir, "resourceDiscoverScript3")
      Files.write("echo foo1", file1, StandardCharsets.UTF_8)
      JavaFiles.setPosixFilePermissions(file1.toPath(),
        EnumSet.of(OWNER_READ, OWNER_EXECUTE, OWNER_WRITE))
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, file1.getPath())

      val error = intercept[SparkException] {
        ResourceDiscoverer.findResources(sparkconf, false)
      }.getMessage()

      assert(error.contains("The gpu discover script threw exception"))
    }
  }

  test("Resource discoverer script doesn't exist") {
    val sparkconf = new SparkConf

    val file1 = new File("/tmp/bogus")
    try {
      sparkconf.set(SPARK_EXECUTOR_RESOURCE_PREFIX + "gpu" +
        SPARK_RESOURCE_DISCOVERY_SCRIPT_POSTFIX, file1.getPath())

      val error = intercept[SparkException] {
        ResourceDiscoverer.findResources(sparkconf, false)
      }.getMessage()

      assert(error.contains("discover gpu's doesn't exist"))
    } finally {
      JavaFiles.deleteIfExists(file1.toPath())
    }
  }

}

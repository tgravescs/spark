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

import org.apache.spark.annotation.Evolving

/**
 * Class to hold information about a type of Resource. A resource could be a gpu, fpga, numa, etc.
 * The array of addresses are resource specific and describe how to access the resource.
 * For instance, for gpus the addresses would be the indices of the gpus.
 *
 * @param name the name of the resource
 * @param units the units of the resources, can be an empty string if units don't apply
 * @param count the number of resources available
 * @param addresses an optional array of strings describing the addresses of the resource
 */
@Evolving
class ResourceInformation(
    private val name: String,
    private val units: String,
    private val count: Long,
    private val addresses: Array[String] = Array.empty) extends Serializable {

  def getName(): String = name
  def getUnits(): String = units
  def getCount(): Long = count
  def getAddresses(): Array[String] = addresses
}

object ResourceInformation {
  // known types of resources
  final val GPU: String = "gpu"

  // known resource type parameters
  final val GPU_COUNT: String = "gpu.count"


}
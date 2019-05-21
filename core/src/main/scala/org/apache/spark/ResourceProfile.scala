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
 * Class to hold information about the executor resources required when this rdd
 * is executed.
 *
 * @param name the name of the resource
 * @param units the units of the resources, can be an empty string if units don't apply
 * @param count the number of resources available
 * @param addresses an optional array of strings describing the addresses of the resource
 */
@Evolving
class ResourceProfile(taskReqs: Map[String, TaskResourceRequirements], sparkConf: SparkConf) {

  // TODO - want to add both task requirements and executor requirements
  // Perhaps use sparkConf as a way???

  private var taskResources: Map[String, TaskResourceRequirements] =
    Map.empty[String, TaskResourceRequirements]

  // TODO - what about number of hosts?  or having different hosts with different profiles?
  // limit the number of tasks running per stage??

  def getResources: Map[String, TaskResourceRequirements] = taskResources

  def required(name: String, value: Long, units: String): Unit = {

  }

  def prefer(name: String, value: Long, units: String): Unit = {

  }
}

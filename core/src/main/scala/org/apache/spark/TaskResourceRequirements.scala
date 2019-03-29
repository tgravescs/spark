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

import scala.collection.Map

/**
 * Resource requirements for each task.
 */
case class TaskResourceRequirements(
    numCores: Int,
    accelerators: Map[String, Int] = Map.empty) extends Serializable

object TaskResourceRequirements {

  def parse(conf: SparkConf): TaskResourceRequirements =
    TaskResourceRequirements(conf.getInt("spark.task.cpus", 1),
      conf.getAllWithPrefix("spark.task.accelerator.")
        .map { case (k, v) if k.endsWith(".count") => (k.stripSuffix(".count"), v.toInt) }.toMap)
}

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

package org.apache.spark.scheduler

import scala.collection.mutable.Map

/**
 * This class is used to store assigned resource to a single container by
 * resource types.
 *
 * Assigned resource could be list of String
 *
 * For example, we can assign container to:
 * "numa": ["numa0"]
 * "gpu": ["0", "1", "2", "3"]
 * "fpga": ["1", "3"]
 *
 * This will be used for NM restart container recovery.
 */
private[spark] class ResourceMapping(
   val resources: Map[String, Array[String]]) {

   def getAssignedResources(resourceType: String): Array[String] = {
     return resources.get(resourceType).getOrElse(Array.empty)
   }

   def addAssignedResource(resourceType: String, values: Array[String]): Unit = {
     resources += resourceType -> values
   }
}

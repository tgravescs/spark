#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark import ExecutorResourceRequest
from pyspark import SparkContext
from pyspark import TaskResourceRequest

class ResourceProfile(object):

    """
    .. note:: Evolving

    Resource profile to associate with an RDD. A ResourceProfile allows the user to
    specify executor and task requirements for an RDD that will get applied during a
    stage. This allows the user to change the resource requirements between stages.

    Only support a subset of the resources for now. The config names supported correspond to the
    regular Spark configs with the prefix removed. For instance overhead memory in this api
    is memoryOverhead, which is spark.executor.memoryOverhead with spark.executor removed.
    Resources like GPUs are resource.gpu (spark configs spark.executor.resource.gpu.*)

    Executor:
      memory - heap
      memoryOverhead
      pyspark.memory
      cores
      resource.[resourceName] - GPU, FPGA, etc

    Task requirements:
      cpus
      resource.[resourceName] - GPU, FPGA, etc
    """
    _jvm = None

    def __init__(self):
        """Create a new ResourceProfile that wraps the underlying JVM object."""
        _jvm=SparkContext._jvm
        self._javaResourceProfile = _jvm.org.apache.spark.resource.ResourceProfile()

    def require(self, resourceRequest):
        if isinstance(resourceRequest, TaskResourceRequest):
            self._javaResourceProfile.require(resourceRequest._javaTaskResourceRequest)
        else:
            self._javaResourceProfile.require(resourceRequest._javaExecutorResourceRequest)

    @property
    def taskResources(self):
        taskRes = self._javaResourceProfile.taskResourcesJMap()
        result = {}
        # convert back to python TaskResourceRequest
        for k, v in taskRes.items():
            result[k] = TaskResourceRequest(v.resourceName(), v.amount())
        return result

    @property
    def executorResources(self):
        execRes = self._javaResourceProfile.executorResourcesJMap()
        result = {}
        # convert back to python ExecutorResourceRequest
        for k, v in execRes.items():
            result[k] = ExecutorResourceRequest(v.resourceName(), v.amount(), v.units(), v.discoveryScript(), v.vendor())
        return result


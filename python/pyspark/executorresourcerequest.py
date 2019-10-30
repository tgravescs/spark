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

from pyspark import SparkContext

class ExecutorResourceRequest(object):

    """
    .. note:: Evolving

    An executor resource request. This is used in conjuntion with the ResourceProfile to
    programmatically specify the resources needed for an RDD that will be applied at the
    stage level.

    @param resourceName Name of the resource
    @param amount Amount requesting
    @param units Units of amount for things like Memory, default is no units, only byte
        types (b, mb, gb, etc) are supported.
    @param discoveryScript Script used to discovery the resources
    @param vendor Vendor, required for some cluster managers
    """
    _jvm = None

    def __init__(self, resourceName, amount, units=None, discoveryScript=None, vendor=None):
        """Create a new ExecutorResourceRequest that wraps the underlying JVM object."""
        _jvm=SparkContext._jvm
        self._javaExecutorResourceRequest = _jvm.org.apache.spark.resource.ExecutorResourceRequest(resourceName, amount, units, discoveryScript, vendor)

    @property
    def resourceName(self):
        return self._javaExecutorResourceRequest.resourceName()

    @property
    def amount(self):
        return self._javaExecutorResourceRequest.amount()

    @property
    def units(self):
        return self._javaExecutorResourceRequest.units()

    @property
    def discoveryScript(self):
        return self._javaExecutorResourceRequest.discoveryScript()

    @property
    def vendor(self):
        return self._javaExecutorResourceRequest.vendor()

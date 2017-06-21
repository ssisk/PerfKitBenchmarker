# Copyright 2017 PerfKitBenchmarker Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Module containing class for GCP's dataflow service.

No Clusters can be created or destroyed, since it is a managed solution
See details at: https://cloud.google.com/dataflow/
"""

from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util


FLAGS = flags.FLAGS

GCP_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


class GcpDpbLocal(dpb_service.BaseDpbService):
  """Object representing service run locally for dpb - DirectRunner."""

  CLOUD = providers.GCP
  SERVICE_TYPE = 'local'

  def __init__(self, dpb_service_spec):
    super(GcpDpbLocal, self).__init__(dpb_service_spec)
    self.project = None

  @staticmethod
  def _GetStats(stdout):
    """
    TODO(saksena): Hook up the metrics API of dataflow to retrieve performance
    metrics when available
    """
    pass

  def Create(self):
    """See base class."""
    pass

  def Delete(self):
    """See base class."""
    pass

  def SubmitJob(self, jarfile, classname, job_poll_interval=None,
                job_arguments=None, job_stdout_file=None,
                job_type=None):
    """See base class."""

    if job_type == self.BEAM_JOB_TYPE:
      full_cmd, beam_dir = beam_benchmark_helper.BuildMavenCommand(
          self.spec, classname, job_arguments)
      stdout, _, retcode = vm_util.IssueCommand(full_cmd, cwd=beam_dir,
                                                timeout=FLAGS.beam_it_timeout)
      assert retcode == 0, "Integration Test Failed."
      return

    cmd = []

    # Needed to verify java executable is on the path
    java_executable = 'java'
    if not vm_util.ExecutableOnPath(java_executable):
      raise errors.Setup.MissingExecutableError(
          'Could not find required executable "%s"' % java_executable)
    cmd.append(java_executable)

    cmd.append('-cp')
    cmd.append(jarfile)

    cmd.append(classname)
    cmd += job_arguments

    stdout, _, _ = vm_util.IssueCommand(cmd)

  def SetClusterProperty(self):
    pass

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

"""Generic benchmark running Apache Beam Integration Tests as benchmarks.

This benchmark provides the piping necessary to run Apache Beam Integration
Tests as benchmarks. It provides the minimum additional configuration necessary
to get the benchmark going.
"""

import copy
import datetime
import json
import tempfile


from perfkitbenchmarker import beam_benchmark_helper
from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import kubernetes_helper
from perfkitbenchmarker import sample
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.dpb_service import BaseDpbService

BENCHMARK_NAME = 'beam_integration_benchmark'

BENCHMARK_CONFIG = """
beam_integration_benchmark:
  description: Run word count on dataflow and dataproc
  dpb_service:
    service_type: dataflow
    worker_group:
      vm_spec:
        GCP:
          machine_type: n1-standard-1
          boot_disk_size: 500
        AWS:
          machine_type: m3.medium
      disk_spec:
        GCP:
          disk_type: nodisk
        AWS:
          disk_size: 500
          disk_type: gp2
    worker_count: 2
    kubernetes_scripts: []
    static_pipeline_options: []
    dynamic_pipeline_options: []
"""

DEFAULT_JAVA_IT_CLASS = 'org.apache.beam.examples.WordCountIT'
DEFAULT_PYTHON_IT_MODULE = 'apache_beam.examples.wordcount_it_test'

flags.DEFINE_string('beam_it_class', None, 'Path to IT class')
flags.DEFINE_string('beam_it_args', None, 'Args to provide to the IT.'
                    'Deprecated & replaced by beam_it_options')
flags.DEFINE_string('beam_it_options', None, 'Pipeline Options sent to the'
                    'integration test.')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites(benchmark_config_spec):
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.errors.Config.InvalidValue: If no Beam args are provided.
    NotImplementedError: If an invalid runner is specified.
  """
  if FLAGS.beam_it_options is None and FLAGS.beam_it_args is None:
    raise errors.Config.InvalidValue(
        'No options provided. To run with default class (WordCountIT), must'
        ' provide --beam_it_options=--tempRoot=<temp dir,'
        ' e.g. gs://my-dir/temp>.')
  if FLAGS.beam_sdk is None:
    raise errors.Config.InvalidValue(
        'No sdk provided. To run Beam integration benchmark, the test must'
        'specify which sdk is used in the pipeline. For example, java/python.')
  if benchmark_config_spec.dpb_service.service_type != dpb_service.DATAFLOW:
    raise NotImplementedError('Currently only works against Dataflow.')
  if not FLAGS.kubectl:
    raise Exception('Please provide path to kubectl tool using --kubectl '
                    'flag. Exiting.')
  if not FLAGS.kubeconfig:
    raise Exception('Please provide path to kubeconfig using --kubeconfig '
                    'flag. Exiting.')
  if (len(FLAGS.beam_it_options) > 0 and
          (not FLAGS.beam_it_options.endswith(']') or
           not FLAGS.beam_it_options.startswith('['))):
    raise Exception("beam_it_options must be of form"
                    " [\"--option=value\",\"--option2=val2\"]")

def KubernetesCreate(file_list):
  """
  Note that this does not try to wait for the creation to succeed - you'll need to have something else that waits
  :param file_list: 
  :return: 
  """
  for file in file_list:
    kubernetes_helper.CreateFromFile(file)

def Prepare(benchmark_spec):
  beam_benchmark_helper.InitializeBeamRepo(benchmark_spec)
  KubernetesCreate(benchmark_spec.dpb_service.kubernetes_scripts)
  pass


def GetStaticPipelineOptions(options_list):
  options = []
  for option in options_list:
    if not len(option.keys()) == 1:
      raise Exception('static_pipeline_options should only have 1 key/value')
    option_kv = option.items()[0]
    options.append((option_kv[0], option_kv[1]))

  return options


def GenerateAllPipelineOptions(it_args, it_options, static_pipeline_options,
                               dynamic_pipeline_options):
  """
  :param it_args: options list passed in via FLAGS.beam_it_args
  :param it_options: options list passed in via FLAGS.beam_it_options
  :param static_pipeline_options: options list passed in via
      benchmark_spec.dpb_service.static_pipeline_options
  :param dynamic_pipeline_options: options list passed in via
      benchmark_spec.dpb_service.dynamic_pipeline_options
  :return: a list of values of the form "\"--option_name=value\""
  """
  # beam_it_options are in [--option=value,--option2=val2] form
  user_option_list = []
  if it_options is not None and len(it_options) > 0:
    user_option_list = it_options.rstrip(']').lstrip('[').split(',')
    user_option_list = [option.rstrip('"').lstrip('"')
                        for option in user_option_list]


  # Add static options from the benchmark_spec
  benchmark_spec_option_list = (
      EvaluateDynamicPipelineOptions(dynamic_pipeline_options))
  benchmark_spec_option_list.extend(
      GetStaticPipelineOptions(static_pipeline_options))
  option_list = ['--{}={}'.format(t[0], t[1])
                 for t in benchmark_spec_option_list]

  # beam_it_args is the old way of passing parameters
  args_list = []
  if it_args is not None and len(it_args) > 0:
    args_list = it_args.split(',')

  return ['"{}"'.format(arg)
          for arg in args_list + user_option_list + option_list]


def Run(benchmark_spec):
  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='beam_integration_benchmark',
                                            delete=False)
  stdout_file.close()

  if FLAGS.beam_it_class is None:
    if FLAGS.beam_sdk == beam_benchmark_helper.BEAM_JAVA_SDK:
      classname = DEFAULT_JAVA_IT_CLASS
    elif FLAGS.beam_sdk == beam_benchmark_helper.BEAM_PYTHON_SDK:
      classname = DEFAULT_PYTHON_IT_MODULE
    else:
      raise NotImplementedError('Unsupported Beam SDK: %s.' % FLAGS.beam_sdk)
  else:
    classname = FLAGS.beam_it_class

  job_arguments = GenerateAllPipelineOptions(
      FLAGS.beam_it_args, FLAGS.beam_it_options,
      benchmark_spec.dpb_service.spec.static_pipeline_options,
      benchmark_spec.dpb_service.spec.dynamic_pipeline_options)

  job_type = BaseDpbService.BEAM_JOB_TYPE

  results = []
  metadata = copy.copy(dpb_service_instance.GetMetadata())

  start = datetime.datetime.now()
  dpb_service_instance.SubmitJob('', classname,
                                 job_arguments=job_arguments,
                                 job_stdout_file=stdout_file,
                                 job_type=job_type)
  end_time = datetime.datetime.now()
  run_time = (end_time - start).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  return results


def KubernetesDeleteAllFiles(file_list):
  for file in file_list:
    kubernetes_helper.DeleteFromFile(file)

def Cleanup(benchmark_spec):
  KubernetesDeleteAllFiles(benchmark_spec.dpb_service.kubernetes_scripts)
  pass


def EvaluateDynamicPipelineOptions(dynamic_options):
    """
    Takes the user's dynamic args and retrieves the information to fill them in.

    dynamic_args is a python map of argument name -> {type, kubernetesSelector}
    returns a list of tuples containing (argName, argValue)

    """

    filledOptions = []
    for optionDescriptor in dynamic_options:
      fillType = optionDescriptor['type']
      optionName = optionDescriptor['name']

      if not fillType:
        raise errors.Config.InvalidValue(
            'For dynamic arguments, you must provide a "type"')

      if fillType == 'NodePortIp':
        argValue = RetrieveNodePortIp(optionDescriptor)
      elif fillType == 'LoadBalancerIp':
        argValue = RetrieveLoadBalancerIp(optionDescriptor)
      elif fillType == 'TestValue':
        argValue = optionDescriptor['value']
      else:
        raise errors.Config.InvalidValue(
            'Unknown dynamic argument type: %s' % (fillType))

      filledOptions.append((optionName, argValue))

    return filledOptions


def KubernetesGet(resource, resourceInstanceName, filter, jsonFilter):
  # TODO - this needs to wait+loop until we have a value returned or timeout
  get_pod_cmd = [FLAGS.kubectl, '--kubeconfig=%s' % FLAGS.kubeconfig,
                 'get', resource, resourceInstanceName, filter,
                 '-o jsonpath', jsonFilter]
  stdout, _, _ = vm_util.IssueCommand(get_pod_cmd, suppress_warning=True)
  pod = json.loads(stdout)
  return pod.get('status', {}).get('podIP', None)


def RetrieveNodePortIp(argDescriptor):
  fillSelector = argDescriptor['selector']
  if not fillSelector:
    raise errors.Config.InvalidValue('For NodePortIp arguments, you must'
                                     ' provide a "selector"')
  # TODO - conflation of names: filter/selector
  return KubernetesGet('pods', '', fillSelector, '.items[0].status.podIP')

  # kubectl get pods -l 'component=elasticsearch' \
  #   -o jsonpath={.items[0].status.podIP}


def RetrieveLoadBalancerIp(argDescriptor):
  serviceName = argDescriptor['serviceName']
  if not serviceName:
    raise errors.Config.InvalidValue('For LoadBalancerIp arguments, you must'
                                     'provide a "serviceName"')
  return KubernetesGet('svc', serviceName, '',
                       '.status.loadBalancer.ingress[0].ip')

# kubectl get svc elasticsearch-external \
#   -o jsonpath='{.status.loadBalancer.ingress[0].ip}'



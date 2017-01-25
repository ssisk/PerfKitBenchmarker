import copy
import datetime
import tempfile

from perfkitbenchmarker import configs
from perfkitbenchmarker import dpb_service
from perfkitbenchmarker import errors
from perfkitbenchmarker import flags
from perfkitbenchmarker import sample

BENCHMARK_NAME = 'dpb_wordcount_benchmark'

BENCHMARK_CONFIG = """
dpb_wordcount_benchmark:
  description: Run WordCount against Beam services.
  dpb_service:
    service_type: dataproc     # These are defaults and can be
    runner_type: spark         # overridden on the command line.
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
"""

WORDCOUNT_CLASSNAME = 'org.apache.beam.examples.WordCount'
WORDCOUNT_DEFAULT_INPUT = 'gs://dataflow-samples/shakespeare/*'

flags.DEFINE_string('dpb_wordcount_input', WORDCOUNT_DEFAULT_INPUT, 'Input for WordCount')
flags.DEFINE_string('dpb_wordcount_out_base', None,
                    'Base directory for word count output')

FLAGS = flags.FLAGS


def GetConfig(user_config):
  return configs.LoadConfig(BENCHMARK_CONFIG, user_config, BENCHMARK_NAME)


def CheckPrerequisites():
  """Verifies that the required resources are present.

  Raises:
    perfkitbenchmarker.data.ResourceNotFound: On missing resource.
  """
  if config.runner_type is not 'dataflow' and config.service_type is 'dataflow':
    raise errors.Config.InvalidValue('Only DataflowRunner can run on Dataflow Service.')
  elif config.runner_type is 'DataflowRunner' and config.service_type is not 'dataflow':
    raise errors.Config.InvalidValue('DataflowRunner can only run on Dataflow Service.')


def Prepare(benchmark_spec):
  pass


def Run(benchmark_spec):

  # Get handle to the dpb service
  dpb_service_instance = benchmark_spec.dpb_service

  # Create a file handle to contain the response from running the job on
  # the dpb service
  stdout_file = tempfile.NamedTemporaryFile(suffix='.stdout',
                                            prefix='dpb_wordcount_benchmark',
                                            delete=False)
  stdout_file.close()

  # Set job submission parameters
  job_arguments = []
  job_arguments.append('--inputFile={}'.format(FLAGS.dpb_wordcount_input))

  if dpb_service_instance.RUNNER_TYPE == dpb_service.APEX:
    # Set apex-specific parameters (if any) here.
  elif dpb_service_instance.RUNNER_TYPE == dpb_service.DATAFLOW:
    job_arguments.append('--gcpTempLocation={}'.format(
        FLAGS.dpb_dataflow_staging_location))
  elif dpb_service_instance.RUNNER_TYPE == dpb_service.FLINK:
    # Set flink-specific parameters (if any) here.
  elif dpb_service_instance.RUNNER_TYPE == dpb_service.SPARK:
    # Set spark-specific parameters (if any) here.

  results = []
  metadata = copy.copy(dpb_service_instance.GetMetadata())
  metadata.update({'input_location': FLAGS.dpb_wordcount_input})

  start = datetime.datetime.now()
  dpb_service_instance.SubmitJob(WORDCOUNT_CLASSNAME,
                                 job_arguments=job_arguments,
                                 job_stdout_file=stdout_file)
  end_time = datetime.datetime.now()
  run_time = (end_time - start).total_seconds()
  results.append(sample.Sample('run_time', run_time, 'seconds', metadata))
  return results


def Cleanup(benchmark_spec):
  pass

import json
import logging

from perfkitbenchmarker import errors
from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.linux_packages import memcached_server 
from perfkitbenchmarker.memcache_service import MemcacheService


class ElastiCacheMemcacheService(MemcacheService):

  CLOUD = providers.AWS

  def __init__(self, cluster_id, region, node_type, num_servers=1):
    self.cluster_id = cluster_id
    self.region = region
    self.node_type = node_type
    self.num_servers = num_servers
    self.hosts = [] # [(ip, port)]

  def Create(self):
    # Create the cluster
    cmd = ['aws', 'elasticache', 'create-cache-cluster']
    cmd += ['--engine=memcached']
    cmd += ['--cache-cluster-id=%s' % self.cluster_id]
    cmd += ['--num-cache-nodes=%s' % self.num_servers]
    cmd += ['--region=%s' % self.region]
    cmd += ['--cache-node-type=%s' % self.node_type]
    vm_util.IssueCommand(cmd)

    # Wait for the cluster to come up
    cluster_info = self._WaitForClusterUp()

    # Parse out the hosts
    self.hosts = \
        [(node['Endpoint']['Address'], node['Endpoint']['Port'])
         for node in cluster_info['CacheNodes']]
    assert len(self.hosts) == self.num_servers

  def Destroy(self):
    cmd = ['aws', 'elasticache', 'delete-cache-cluster']
    cmd += ['--cache-cluster-id=%s' % self.cluster_id]
    cmd += ['--region=%s' % self.region]
    out, _, _ = vm_util.IssueCommand(cmd)

  def Flush(self):
    vm_util.RunThreaded(memcached_server.FlushMemcachedServer,
                        self.hosts)

  def GetHosts(self):
    return ["%s:%s" % (ip, port) for ip, port in self.hosts]

  def GetMetadata(self):
    return {'num_servers': self.num_servers,
            'elasticache_region': self.region,
            'elasticache_node_type': self.node_type}

  def _GetClusterInfo(self):
    cmd = ['aws', 'elasticache', 'describe-cache-clusters']
    cmd += ['--cache-cluster-id=%s' % self.cluster_id]
    cmd += ['--region=%s' % self.region]
    cmd += ['--show-cache-node-info']
    out, _, _ = vm_util.IssueCommand(cmd)
    return json.loads(out)["CacheClusters"][0]

  @vm_util.Retry(poll_interval=15, timeout=300,
                 retryable_exceptions=(errors.Resource.RetryableCreationError))
  def _WaitForClusterUp(self):
    """Block until the ElastiCache memcached cluster is up.

    Will timeout after 5 minutes, and raise an exception. Before the timeout
    expires any exceptions are caught and the status check is retried.

    We check the status of the cluster using the AWS CLI.

    Returns:
      The cluster info json as a dict

    Raises:
      errors.Resource.RetryableCreationError when response is not as expected or
        if there is an error connecting to the port or otherwise running the
        remote check command.
    """
    logging.info("Trying to get ElastiCache cluster info for %s",
                 self.cluster_id)
    cluster_status = None
    try:
      cluster_info = self._GetClusterInfo()
      cluster_status = cluster_info['CacheClusterStatus']
      if cluster_status == 'available':
        logging.info("ElastiCache memcached cluster is up and running.")
        return cluster_info
    except errors.VirtualMachine.RemoteCommandError as e:
      raise errors.Resource.RetryableCreationError(
          "ElastiCache memcached cluster not up yet: %s." % str(e))
    else:
      raise errors.Resource.RetryableCreationError(
          "ElastiCache memcached cluster not up yet. Status: %s" %
          cluster_status)

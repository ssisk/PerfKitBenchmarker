from perfkitbenchmarker import providers
from perfkitbenchmarker import vm_util
from perfkitbenchmarker.memcache_service import MemcacheService

class MemcacheService(MemcacheService):

  CLOUD = providers.GCP

  def __init__(self):
    pass

  def Create(self):
    raise NotImplementedError

  def Destroy(self):
    raise NotImplementedError

  def Flush(self):
    raise NotImplementedError

  def GetHosts(self):
    raise NotImplementedError

  def GetMetadata(self):
    raise NotImplementedError

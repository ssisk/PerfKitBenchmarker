

class MemcacheService(object):
  CLOUD = None

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

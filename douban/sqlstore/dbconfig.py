import os
import json

from douban.utils import read_object
from douban.utils.config import config_dir as _config_root

class Error(Exception):
    pass

class InvalidConfig(Error):
    pass

class DBConfig(dict):
    config_dir = _config_root + '/sqlstore'
    
    def __init__(self, name):
        dict.__init__(self)
        try:
            self.update(read_object(os.path.join(self.config_dir,name)))
        except ValueError, e:
            raise InvalidConfig(e)

from enums import *

def is_snow_supported_os(os):
  return os  in [OperatingSystem.centos_7, OperatingSystem.rhel_8]

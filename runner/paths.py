import os
from paver.easy import *

class Paths:
    def __init__(self, options):
        operating_system = options.operating_system.name.lower()
        host_database = options.host_database.name.lower()
        building_type = options.building_type.name.lower()
        self.runner = os.path.realpath(os.path.expanduser(options.get('runner')['dir']))
        self.dev = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/..')
        self.home = f'{self.runner}/home/host_user/'
        self.scripts = f'{self.dev}/docker/scripts'
        self.build = f'{self.runner}/build/{operating_system}/{host_database}/{building_type}'
        self.artifacts = f'{self.runner}/artifacts/{operating_system}/{host_database}'
        self.ccache = f'{self.runner}/ccache'
        self.log = f'{self.artifacts}/log'
        self.pgconf = f'{self.artifacts}/pgconf'
        self.vimrc = f'{self.dev}/tools/vim/vimrc'
        self.vimspector = f'{self.dev}/tools/vim/vimspector.json'
        self.nvim = f'{self.dev}/tools/nvim'
        sncprovision_clone_dir = options.get('snc-provision')['clone_dir'] or self.runner
        self.sncprovision = f'{sncprovision_clone_dir}/snc-provision'
        self.mac_native = f'{self.runner}/mac_native'
        self.mac_glide = f'{self.runner}/mac_glide'
        self.pg_data = '/pg-data'
        self.pg_data_primary = '/pg-data-primary'
        self.pg_data_standby = '/pg-data-standby'
        self.pg_data_read_replica = '/pg-data-read-replica'
        self.tests = '/tests'
        self.glide_psql = '/glide/psql'

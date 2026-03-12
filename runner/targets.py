from paver.easy import *
from enums import *
import cmake
import instance
import all_options
import logging
import atexit

LOG = logging.getLogger(__name__)

def run_variant(options, tasks, name):
    try:
        all_options.apply_changes(options)
    except Exception as err:
        LOG.warning(err)
        return
    for task in tasks:
        LOG.info(f'{task} for {name}')
        call_task(task, args={})
    atexit._run_exitfuncs()

@task
@consume_args
def all(options, args):
    operating_systems = list(OperatingSystem.__members__.keys())
    host_databases = list(HostDatabase.__members__.keys())

    for operating_system in operating_systems:
        for host_database in host_databases:
            options['operating-system'] = operating_system
            options['host-database'] = host_database
            run_variant(options, args, f'{operating_system}/{host_database}')

@task
@consume_args
def build_types(options, args):
    build_types = list(BuildingType.__members__.keys())

    for build_type in build_types:
        options['building']['type'] = build_type
        run_variant(options, args, build_type)

import argparse
import logging
import os
from jinja2 import Environment, FileSystemLoader
from enums import *
from paver.easy import consume_args, task

LOG = logging.getLogger(__name__)

@task
@consume_args
def make_config(options, args):
    """Generate override_configuration.yaml"""

    operating_systems = list(OperatingSystem.__members__.keys())
    build_types = list(BuildingType.__members__.keys())
    host_databases = list(HostDatabase.__members__.keys())

    parser = argparse.ArgumentParser()
    parser.add_argument('--force', action='store_true', help='Always write a config file.')

    parser.add_argument('--runner-dir', required=True, help='The build directory')
    parser.add_argument('--clean', action='store_const', const='deterministic', default='nothing', help='Clean data, build, and artifacts before building.')
    parser.add_argument('--operating-system', required=True, choices=operating_systems)
    parser.add_argument('--build-type', required=True, choices=build_types)
    parser.add_argument('--host-database', required=True, choices=host_databases)
    parser.add_argument('--postfix', required=False, default = "")
    parser.add_argument('--package-kind', required=False, default = "database")
    parser.add_argument('--snc-provision-url', required=False, default = "")
    parser.add_argument('--snc-provision-clone-dir', required=False, default = "")

    task_args, _ = parser.parse_known_args(args)

    runner_dir = os.path.join(options.paths.dev, 'runner')

    target_file = f'{runner_dir}/override_configuration.yaml'
    if not task_args.force and os.path.exists(target_file):
        raise OSError("{} already exists, use --force to overwrite".format(target_file))

    args_dict = vars(task_args)
    env = Environment(loader=FileSystemLoader(runner_dir))
    with open(target_file, 'w') as file:
        file.write(env.get_template('override_configuration_template.yaml').render(args_dict))

    LOG.info('override_configuration.yaml file generated')

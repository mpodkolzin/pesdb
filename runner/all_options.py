import logging
import sys
import os
import platform
import collections.abc
from ruamel import yaml
from copy import deepcopy
from enums import *
from paths import *

def reset_called():
    all_tasks = environment.get_tasks()
    for task in all_tasks:
        task.called = False

def update_nested_dict(d, other):
    for k, v in other.items():
        d_v = d.get(k)
        if isinstance(v, collections.abc.Mapping) and isinstance(d_v, collections.abc.Mapping):
            update_nested_dict(d_v, v)
        else:
            d[k] = deepcopy(v)

def update_nested_options(d, options):
    for k, v in options.items():
        d_v = d.get(k)
        if (isinstance(v, Bunch) and isinstance(d_v, collections.abc.Mapping)):
            update_nested_options(d_v, v)
        else:
            d[k] = deepcopy(v)

def load(options):
    parser = yaml.YAML(typ="safe", pure=True)
    # improve this -> match the task so that we do not 'only' have global options
    dirname = os.path.join(os.path.join(
        os.path.dirname(os.path.realpath(__file__))))
    with open(os.path.join(dirname, 'global_configuration.yaml')) as global_config:
        config_data = parser.load(global_config)
    override_path = os.path.join(dirname, 'override_configuration.yaml')
    if os.path.exists(override_path):
        with open(os.path.join(dirname, 'override_configuration.yaml')) as override_config:
            user_config_data = parser.load(override_config)
        if user_config_data:
            update_nested_dict(config_data, user_config_data)
    else:
        print("Warning: override_configuration.yaml not found")

    update_nested_options(config_data, options)

    options.update(config_data)

def parse_enum(enum_type, value, option_name):
    try:
        return enum_type[value]
    except KeyError:
        print(f"Invalid option for {option_name}.")
        print(f"Valid values: {', '.join(enum_type.__members__.keys())}")
        if enum_type.__doc__:
            print(f"{enum_type.__doc__}")
        raise

def apply_changes(options):
    runner_options = options.get('runner')
    if not runner_options['exception-traces']:
        sys.tracebacklimit = 0

    # map enums
    options.operating_system = parse_enum(OperatingSystem, options.get('operating-system'), 'operating-system')
    options.host_database = parse_enum(HostDatabase, options.get('host-database'), 'host-database')
    options.building_type = parse_enum(BuildingType, options.get('building')['type'], 'building.type')
    options.build_cleaning = parse_enum(BuildCleaning, options.get('building')['clean'], 'building.clean')

    options.paths = Paths(options)
    options.teamcity = os.environ.get('TEAMCITY') != None
    options.no_tty = os.environ.get('NO_TTY') != None
    options.mcp_runner = os.environ.get('MCP_RUNNER') != None

    # set default architecture if user doesn't provide one in the cli or the configuration file
    if options.get('architecture') is None:
        options.architecture=Architecture.arm64 if sys.platform == "darwin" and platform.machine() == "arm64" else Architecture.amd64

    options.container_architecture = options.architecture.get_docker_platform()

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.getLevelName(runner_options['log-level']))

    # ensure the OS/host-db combination actually is supported
    operating_system = options.operating_system.name.lower()
    host_database = options.host_database.name.lower()
    if not os.path.isfile(f'{options.paths.dev}/docker/{operating_system}/{host_database}'):
        raise Exception(f'Combination of {operating_system}/{host_database} does not exist (yet)')

    # reset all called tasks as we changed the options and expect we call them with the new options now.
    reset_called()

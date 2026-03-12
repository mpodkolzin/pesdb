import os
import pwd
import logging
import docker
import stat
import sys
import atexit
import subprocess
import tqdm
import psutil
import re
import credential
import mac
import signal

from enums import BuildingType, OperatingSystem, BuildCleaning

from paver.easy import call_task, task, cmdopts
from pathlib import Path
from docker.errors import NotFound
from mcp_runner.decorators import *

from utils import *


LOG = logging.getLogger(__name__)
instance = None
glide_pg_conf_dir = None
MacNative = "MyMacInstance" #anything will suffice
VOLUME_BUILD = "build"
VOLUME_CCACHE = "ccache"
VOLUME_PG_DATA = "pg-data"
VOLUME_PG_DATA_PRIMARY = "pg-data-primary"
VOLUME_PG_DATA_READ_REPLICA = "pg-data-read-replica"
VOLUME_PG_DATA_STANDBY = "pg-data-standby"
VOLUME_TESTS =  "tests"
VOLUME_GLIDE_PSQL = "glide-psql"

def _get_volume_prefix(options):
    runner_dir = re.sub(r"[^a-zA-Z0-9]", "_", options.paths.runner).lower()
    operating_system = options.operating_system.name.lower()
    host_database = options.host_database.name.lower()
    return f"vol_{runner_dir}_{operating_system}_{host_database}"

# only meant for low volume writes
def _add_bind(volumes, host_dir, docker_dir, readonly=False):
    absolute_host_dir = os.path.abspath(os.path.expanduser(host_dir))
    absolute_docker_dir = os.path.expanduser(docker_dir)
    volumes[absolute_host_dir] = {'bind': absolute_docker_dir, 'mode': readonly and 'ro' or 'rw'}
    LOG.debug(f"adding bind {absolute_host_dir} for {absolute_docker_dir}")

# docker local mappings
def _add_volume(client, prefix, volumes, volume_name, docker_dir):
    full_volume_name = f"{prefix}_{volume_name}";
    try:
        volume = client.volumes.get(full_volume_name)
    except NotFound:
        LOG.debug("Docker volume '%s' doesn't exist yet, creating", full_volume_name)
        client.volumes.create(name=full_volume_name)

    absolute_docker_dir = os.path.expanduser(docker_dir)
    volumes[full_volume_name] = {'bind': absolute_docker_dir, 'mode': 'rw'}
    LOG.debug(f"adding volume {full_volume_name} for {absolute_docker_dir}")

def _get_volumes(client, options):
    global glide_pg_conf_dir
    volumes = {}
    binds = {}

    prefix = _get_volume_prefix(options)

    # home dir may have custom settings/scripts, so don't use volume, as otherwise they are gone after each clean build.
    _add_bind(binds, options.paths.home, "/home/host_user")

    # map the full dir, not the build type specific dir
    _add_volume(client, prefix, volumes, VOLUME_BUILD, f"{options.paths.runner}/build")
    _add_volume(client, prefix, volumes, VOLUME_CCACHE, options.paths.ccache)
    _add_volume(client, prefix, volumes, VOLUME_PG_DATA, options.paths.pg_data)
    _add_volume(client, prefix, volumes, VOLUME_PG_DATA_PRIMARY, options.paths.pg_data_primary)
    _add_volume(client, prefix, volumes, VOLUME_PG_DATA_STANDBY, options.paths.pg_data_standby)
    _add_volume(client, prefix, volumes, VOLUME_PG_DATA_READ_REPLICA, options.paths.pg_data_read_replica)
    _add_volume(client, prefix, volumes, VOLUME_TESTS, options.paths.tests)
    _add_volume(client, prefix, volumes, VOLUME_GLIDE_PSQL, options.paths.glide_psql)

    _add_bind(binds, options.paths.dev, options.paths.dev)

    # map things to outside we want to easily inspect
    _add_bind(binds, options.paths.log, '/var/log')
    _add_bind(binds, options.paths.runner, options.paths.runner)

    _add_bind(binds, options.paths.vimrc, '/usr/local/share/vim/vimrc')
    _add_bind(binds, options.paths.vimspector, '/usr/local/share/vim/bundle/vimspector/configurations/linux/_all/vimspector.json')
    _add_bind(binds, options.paths.nvim, '/home/host_user/.config/nvim')
    # ensure up-to-date version of scripts
    _add_bind(binds, options.paths.scripts, '/swarm')

    _add_snc_provision_volumes(options, binds)

    for _, path_map_dirs in options['docker']['paths'].items():
        _add_bind(binds, path_map_dirs[0], path_map_dirs[1])

    # so that the limits also apply inside the container
    if not sys.platform == "darwin":
        _add_bind(volumes, '/etc/security/limits.conf', '/etc/security/limits.conf')

    return {**volumes, **binds}

def _add_snc_provision_volumes(options, binds):
    snc_provision_path = options.paths.sncprovision
    # avoid repo being modified in the container as root that making it unusable on the host without root permission
    _add_bind(binds, snc_provision_path, '/glide/snc-provision', True)
    # these paths need to be writable, they are also ignored by git
    # NOTE: these paths must also be in docker/scripts/fix_permission.sh
    sncprovision_writeable_dirs = [
        '.rbenv/linux-2.6-libc-2.5-x86_64/shims',
        'puppet/etc/devices',
        'puppet/var',
        'puppet/etc/ssl',
        'tmp'
    ]
    for sncprovision_subdir in sncprovision_writeable_dirs:
        subdir_hostpath = Path(snc_provision_path + '/' + sncprovision_subdir)
        # Avoid mapping non-existing paths in the host into the container as those will be created as root owned
        # dirs in the host. Otherwise there will be undeletable folder in the host.
        if os.path.exists(subdir_hostpath.parent):
            _add_bind(binds, snc_provision_path + '/' + sncprovision_subdir, '/glide/snc-provision/' + sncprovision_subdir)

def cleanup_volumes(options):
    if instance and instance == MacNative:
        return
    LOG.info('Cleaning up instance volumes.')
    client = docker.from_env()
    prefix = _get_volume_prefix(options)
    for container in client.containers.list(all=True):
        for mount in container.attrs['Mounts']:
            if mount.get('Name') and mount.get('Name').startswith(prefix):
                LOG.info(f"Stopping and removing container {container.id} using volume {mount['Name']}")
                container.stop()
                container.remove()
                break

    build_volumes = [f"{prefix}_{VOLUME_BUILD}", f"{prefix}_{VOLUME_GLIDE_PSQL}"]
    ccache_volume = f"{prefix}_{VOLUME_CCACHE}"
    test_volume = f"{prefix}_{VOLUME_TESTS}"
    data_volumes = [f"{prefix}_{VOLUME_PG_DATA}", f"{prefix}_{VOLUME_PG_DATA_PRIMARY}", f"{prefix}_{VOLUME_PG_DATA_READ_REPLICA}", f"{prefix}_{VOLUME_PG_DATA_STANDBY}"]

    for volume in client.volumes.list():
        if volume.name.startswith(prefix):
            if (options.build_cleaning & BuildCleaning.build_cache) and volume.name == ccache_volume:
                LOG.info(f"Removing build cache volume {volume.name}")
                volume.remove()
            elif (options.build_cleaning & BuildCleaning.data) and volume.name in data_volumes:
                LOG.info(f"Removing data volume {volume.name}")
                volume.remove()
            elif (options.build_cleaning & BuildCleaning.build) and volume.name in build_volumes:
                LOG.info(f"Removing build volume {volume.name}")
                volume.remove()
            elif (options.build_cleaning & BuildCleaning.tests) and volume.name == test_volume:
                LOG.info(f"Removing tests volume {volume.name}")
                volume.remove()

def get_progress_for_chunks(line, chunks):
    if line.get('status', '') in ['Downloading', 'Waiting']:
        cur_total = (0, 0)

        if line.get('progressDetail', {}) != {}:
            cur_total = (line['progressDetail']['current'], line['progressDetail']['total'])

        chunks[line['id']] = cur_total

    return chunks

def _start_docker(options, fixed_image = None):
    global instance, glide_pg_conf_dir, env

    host_user = pwd.getpwuid(os.geteuid())
    repository = options['docker']['repository']
    architecture = options.architecture.name.lower()
    default_image = '{}-{}-dev'.format(
            options.operating_system.name.lower(),
            options.host_database.name.lower())
    image = fixed_image or default_image
    tag = str(options['docker']['image-version'])
    full_image = f'{repository}{image}-{architecture}:{tag}'
    network = options['docker'].get('network-mode', None)
    ports = options.get('docker', {}).get('ports', {})
    name = options.get('docker', {}).get('name', None)
    attach_to = options.get('docker', {}).get('attach_to', None)

    try:
        client = docker.from_env()
    except Exception as e:
        LOG.error("Could not connect to docker daemon; make sure it is running")
        sys.exit(e)

    # Explicitly try to get the image before to warn that we are pulling if we need to
    if not client.images.list(filters={"reference":full_image}):
        LOG.info('Image "%s" does not exist locally, pulling...', full_image)

        if options.teamcity:
            client.images.pull(f'{repository}{image}-{architecture}', tag=tag)
        else:
            api_client = docker.APIClient(version="auto")
            bar = tqdm.tqdm(unit='B', unit_scale=True)
            bar.set_description("Downloading")
            chunks = {}

            for line in api_client.pull(f'{repository}{image}-{architecture}', tag=tag, stream=True, decode=True):
                if get_progress_for_chunks(line, chunks):
                    sums = [sum(x) for x in zip(*chunks.values())]
                    if bar.total != sums[1]:
                        bar.total = sums[1]
                        bar.refresh()
                    bar.n = sums[0]
                    bar.refresh()

        LOG.info('Image "%s" pulled, continuing...', full_image)

    if attach_to:
        LOG.info(f"Trying to attach to {attach_to}")
        try:
            instance = client.containers.get(attach_to)
            return
        except docker.errors.NotFound:
            LOG.info(f'Container with name "{attach_to}" does not exist, starting a new container')
            name = attach_to

    # Check if a container with the same name already exists and kill it
    if name:
        try:
            existing_container = client.containers.get(name)
            LOG.info(f'Container with name "{name}" already exists, stopping and removing it')
            existing_container.remove(force=True)
            LOG.debug(f'Removed existing container "{name}"')
        except docker.errors.NotFound:
            # Container doesn't exist, which is fine
            pass
        except Exception as e:
            LOG.warning(f'Failed to remove existing container "{name}": {e}')

    LOG.debug(f'Starting new docker container for {full_image}')
    try:
        # disable killing whilst container is created, otherwise we get dangling instances
        original_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
        # Only register stop on exit if not in MCP mode with attach_to
        # In MCP mode, the container should persist between runner invocations
        if not (options.mcp_runner and attach_to):
            atexit.register(stop)
        container_args = {
            "image": full_image,
            "command": '/swarm/systemd_mock_init.sh',
            # ptrace needed by gdb, nice needed for process priority changes, perfmon needed by perf.
            "cap_add": ['SYS_PTRACE', 'SYS_NICE', 'PERFMON'],
            "detach": True,
            "environment": env,
            "shm_size": '1024g',
            "user": 'root',
            "volumes": _get_volumes(client, options),
            "tmpfs": {'/tmp': f'uid={host_user.pw_uid}'},
            "ulimits": [docker.types.Ulimit(name='core', soft=-1, hard=-1)],
            "name": name,
            "platform": options.container_architecture,
            # Disable userns-remap to avoid file permission issues with bind mounts
            "userns_mode": "host"
        }
        if network == 'host':
            container_args["network_mode"] = 'host' # needed for iotop to work
        else:
            container_args["network_mode"] = 'bridge'
            container_args["ports"] = ports

        instance = client.containers.create(**container_args)

        signal.signal(signal.SIGINT, original_handler)

        image_info = client.images.get(instance.image.id).attrs
        architecture = image_info.get("Architecture", "Unknown")
        # Start in a second step to still have access to 'instance' in case of failure
        instance.start()
        LOG.info(f'Started new docker instance with name: {instance.name} with arch {architecture} and id {instance.image.id}')
        instance.reload()
    except Exception as e:
        LOG.error(f"Failed to start container: {e}")
        sys.exit(e)

    # Do blocking startup because otherwise the right user etc. might not yet exist
    execute_root(options, ['/swarm/startup.sh &> /dev/null'])

def _start_mac(options):
    global instance, glide_pg_conf_dir, env
    instance = MacNative
    mac.setup(options, env)

    with open(f"{options.paths.mac_native}/env_vars", "w") as f:
        for key, value in env.items():
            f.write(f'export {key}="{value}"\n')

def _create_paths(options):
    for path in list(options.paths.__dict__.values()):
        # require that all is within one directory for safety.
        if not os.path.exists(path) and path.startswith(options.paths.runner):
            os.makedirs(path)
            os.chmod(path, stat.S_IRWXO | stat.S_IRWXG | stat.S_IRWXU)

def _start(options, fixed_image = None):
    global instance, glide_pg_conf_dir, env

    host_user = pwd.getpwuid(os.geteuid())
    env = {
            'LOCAL_USER_ID' : host_user.pw_uid,
            'LOCAL_GROUP_ID' : host_user.pw_gid,
            'DEV_DIR' : options.paths.dev,
            'BUILD_DIR' : options.paths.build,
            'TERM': os.environ.get('TERM'),
            'CCACHE_DIR' : options.paths.ccache,
            'CCACHE_MAXSIZE' : '50G',
            'BUILD_TYPE' : options['building']['type'],
            'USE_VALGRIND' : options['building']['use-valgrind'],
            'ARTIFACTS_DIR' : options.paths.artifacts,
            'CONFIG' : options.get('config'),
            'PG_DATA' : options.paths.pg_data,
            'PG_DATA_PRIMARY' : options.paths.pg_data_primary,
            'PG_DATA_STANDBY' : options.paths.pg_data_standby,
            'PG_DATA_READ_REPLICA' : options.paths.pg_data_read_replica,
            'GLIDE_PSQL' : options.paths.glide_psql,
            'TESTS_DIR' : options.paths.tests,
            'TEAMCITY' : "1" if options.teamcity else "0",
            # Pass nexus crenentials from env variables always disregarding if snc-provision is used or not.
            # The specific runner task should validate if the credentials is required.
            'NEXUS_USER' : credential.get_credential(options, 'NEXUS_USER'),
            'NEXUS_PASSWD' : credential.get_credential(options, 'NEXUS_PASSWD')
          }

    override_env = options['runner'].get('environment', None)
    if override_env and isinstance(override_env, dict):
        env.update(override_env)

    # Disable query deprioritization with Sanitizers build because it relies on setting binary capabilities,
    # which is incompatible with the instrumentation Valgrind or Sanitizers use
    # TODO: remove this once `set_capabilities` in build.py is removed
    if options['building']['type'] == BuildingType.Sanitizers.name:
        env['OVERRIDE_PG_CONFIG'] = "swarm64da.enable_deprioritize_slow_queries = off"

    _create_paths(options)


    if options.operating_system == OperatingSystem.mac_native:
        _start_mac(options)
    else:
        _start_docker(options)

def stop():
    global instance

    if instance and instance != MacNative:
        cleanup_command = ['docker', 'exec', '--user=root', '-t',
               instance.name,
               '/bin/bash', '-c', '/swarm/fix_permission.sh']
        subprocess.check_call(cleanup_command)
        LOG.info("Stopping docker '%s'", instance.name)
        instance.stop()
        LOG.debug('Container logs:\n%s', instance.logs().decode('utf-8'))
        instance.remove()
    instance = None
    atexit.unregister(stop)

def update_limitations(options, max_mem):
    global instance

    if instance == MacNative:
        return

    if max_mem == -1:
        # Signal to use all RAM available
        max_mem = psutil.virtual_memory().total

    instance.update(mem_limit=max_mem, memswap_limit=max_mem)

def instance_is_mac():
    global instance
    return instance == MacNative

def execute(options, commands=['/bin/bash'], workdir='/', run_as_user='host_user',
            fixed_image=None, before_command='source /swarm/env.sh', environments=None, need_privilege=False):
    global instance
    if not instance:
        _start(options, fixed_image)

    if instance != MacNative:
        command_list = [before_command]
        command_list.append(f'cd {workdir}')
        command_list += commands

        term_args = '-it'
        if options.teamcity or options.no_tty:
            term_args = '-t'
        if options.mcp_runner:
            term_args = '-i'

        # For some reason 'instance.exec_run()' does not allow using the shell,
        # so keep it like that for now.
        command = ['docker', 'exec', f'--user={run_as_user}',
                   term_args]
        if need_privilege:
            command.append('--privileged')
        if environments:
            for key, value in environments.items():
                command += ['-e', f'{key}={value}']

        command += [instance.name, '/bin/bash', '-c', ' && '.join(command_list)]

        return subprocess.check_call(command)
    else:
        if before_command != 'source /swarm/env.sh':
            sys.exit(f'unsupported operation for macos, before-commands are not yet implemented')
        dev_dir = options.paths.dev
        command = ['/bin/bash', '-c', f'cd {options.paths.mac_native} && source exports && source env_vars && source {dev_dir}/docker/scripts/env.sh && cd {workdir} && ' + ' && '.join(commands)]
        return subprocess.check_call(command)

def execute_root(options, commands, workdir='/'):
    return execute(options, commands, workdir, run_as_user='root')

# snc-provision repo must be cloned before calling this function
def execute_snow(options, commands=['/bin/bash'], workdir='/', environements=None, need_privilege=True):
    if not is_snow_supported_os(options.operating_system):
        sys.exit(f'snow env is not supported in {options.operating_system.name}')

    if credential.get_credential(options, 'NEXUS_USER') is None or credential.get_credential(options, 'NEXUS_PASSWD') is None:
        sys.exit('Credential NEXUS_USER and NEXUS_PASSWD must be set for snow env.')

    # do not source /swarm/env.sh which populate writable binary path in $PATH that snc-provision warns about.
    execute(options, commands, workdir, run_as_user='root', before_command='source /swarm/snow_env.sh',
            environments=environements, need_privilege=need_privilege)

@task
def shell(options):
    execute(options, ['/bin/bash'], options.paths.build)

@task
@mcp_enabled(interactive = True, prompt_pattern=r"BASH#{3}")
def shell_root(options):
    """Interactive shell in the container"""
    if options.mcp_runner:
        execute_root(options, ['script -q /dev/null -c "/swarm/ai_shell.sh"'], options.paths.build)
    else:
        execute_root(options, ['/bin/bash'], options.paths.build)

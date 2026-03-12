from platform import architecture
from paver.easy import call_task, task, cmdopts, needs
from jinja2 import Environment, FileSystemLoader
from ruamel import yaml

import os
import shlex
import subprocess
import tempfile
from instance import *
import logging
import shutil
import all_options

from enums import *
from utils import *

LOG = logging.getLogger(__name__)

class ContainerContext:
    """Stores commonly used translated values from options to avoid repetitive conversions"""
    def __init__(self, options):
        self.options = options
        self.operating_system = options.operating_system.name.lower()
        self.host_database = options.host_database.name.lower()
        self.architecture = options.architecture.name.lower()
        self.building_type = options.building_type.name.lower()
        self.docker_dir = f'{options.paths.dev}/docker'
        self.docker_files = f'{options.paths.dev}/docker/files'
        self.rhel_version = options.operating_system.rhel_version()
        self.disable_vim = "true" if options.operating_system.name == OperatingSystem.elxr_12.name else "false"

CMDOPTS = [('push', '', 'Push image to registry'), ('version=', 'v', 'Image version'),
           ('raptordb_pro=', '', 'Enable raptordb.pro'),
           (f'{OperatingSystem.rhel_8.name}', '', 'Specify RHEL8'),
           (f'{OperatingSystem.rhel_9.name}', '', 'Specify RHEL9'),
           ('arch=', '', 'architecture')
           ]

def build(ctx, options, name, file, arguments=None, override_version='', override_name='', secrets=[]):
    repository = options['docker']['repository']
    version = override_version if override_version != '' else options['version']
    # put the architecture in the name as somehow SN nexus doesn't (truly) support multiarch containers, and this at least guaranteed works
    image = f'{repository}{name}-{ctx.architecture}:{version}' if override_name == '' else f'{repository}{override_name}:{version}'

    tmp = tempfile.NamedTemporaryFile()
    env = Environment(loader=FileSystemLoader(ctx.docker_dir))
    template = env.get_template(file).render(arguments)
    tmp.write(template.encode())
    tmp.flush()

    # disable container cache on TC to ensure we always get the latest version of the dependencies
    disable_cache = "--no-cache" if options.teamcity else ""
    build_cmd = f"docker build {disable_cache} -t {image} --platform {options.container_architecture} -f {tmp.name} {ctx.docker_dir}"

    if arguments:
        for key, value in arguments.items():
            key = key.upper().replace('-', '_')
            if type(value) is list:
                value = ' '.join(value)
            build_cmd += f' --build-arg {key}="{value}"'

    for secret_key in secrets:
        build_cmd += f' --secret id={secret_key}'

    subprocess.check_call(shlex.split(build_cmd))

    if options.get('push'):
        subprocess.check_call(shlex.split(f'docker push {image}'))

    return image

def create_product_files(ctx, options):
    call_task('build.packages')
    # empty out the docker files dir so we can not by accident use the wrong files
    execute_root(options, [f'rm -rf {ctx.docker_files}'])
    # copy everything to the right place so the dockerfile can use it
    execute_root(options, [f'cp -a {options.paths.build}/packages {ctx.docker_files}'])

def base_image(ctx, options):
    repository = options['docker']['repository']
    version = options['docker']['image-version']

    return f'{repository}{ctx.operating_system}-{ctx.host_database}-{ctx.architecture}:{version}'

def get_rhel_licensed_image(ctx, options):
    if not options.operating_system.is_rhel():
        return ""

    repository = options['docker']['repository']
    rhel_version = options.operating_system.rhel_version()
    image = f'{repository}rhel{rhel_version}-base-{ctx.architecture}:latest'
    LOG.info(f'Using RHEL_LICENCED_IMAGE {image}')
    return image

@task
@cmdopts(CMDOPTS)
def create_developer(options):
    """(Re)creates the container for the currently selected OS/host-database combination"""
    ctx = ContainerContext(options)
    
    arguments = {'HOST_DATABASE': ctx.host_database.upper(),
        'OPERATING_SYSTEM': ctx.operating_system.upper(),
        'RHEL_LICENCED_IMAGE' : get_rhel_licensed_image(ctx, options),
        'ARCH': ctx.architecture,
        'INSTALL_PREFIX': '/usr/local',
        'GLIDE_PREFIX': '/glide',
        'DISABLE_VIM' : ctx.disable_vim
        }

    LOG.info(f'Creating base container for {ctx.operating_system}/{ctx.host_database}')
    base_img = build(ctx, options,
            f'{ctx.operating_system}-{ctx.host_database}',
            f'{ctx.operating_system}/{ctx.host_database}',
            arguments)

    LOG.info(f'Creating developer container for {ctx.operating_system}/{ctx.host_database} {base_img}')

    base_path = os.path.dirname(os.path.realpath(__file__))
    runner_requirements = open(f'{base_path}/requirements.txt', 'r').read()

    arguments['BASE_IMAGE'] = base_img
    arguments['RUNNER_REQUIREMENTS'] = runner_requirements

    # no credentials are needed for now
    secrets = []

    build(ctx, options,
            f'{ctx.operating_system}-{ctx.host_database}-dev',
            f'{ctx.operating_system}/{ctx.host_database}_dev',
            arguments,
            secrets=secrets
    )

@task
@cmdopts(CMDOPTS)
@needs(['ensure_snc_provision_branch'])
def create_glideIT(options):
    """Create docker image for glide IT"""
    ctx = ContainerContext(options)

    if not is_snow_supported_os(options.operating_system):
        sys.exit(f'create glideIT is not supported in {options.operating_system.name}')

    # The docker image name (jenkins-postgres:tembo.dev-unstable) doesn't have distinction on OSes.
    # Only allow push to remote registry from centos 7 to avoid image in the registry being overridden by non desired OS.
    # Change this restriction when the desired version is changed, eg. buidleng changes to run with rhel_8.
    if options.get('push') and (options.operating_system != OperatingSystem.centos_7 or
         options.host_database != HostDatabase.psql_15 or
         options.building_type != BuildingType.Release):
        sys.exit(f'pushing glideIT is not supported for {options.operating_system.name}/{options.host_database.name}/{options.building_type.name}')

    # fix permissions for snc-provision due to fix_permission.sh not being called due to errors from other jobs so that we can copy all files
    execute_root(options, ['source /swarm/fix_permission.sh'])

    call_task('build.packages')
    # cleanup previous files dir
    shutil.rmtree(ctx.docker_files, ignore_errors = True)

    # copy everything to the right place so the dockerfile can use it
    execute_root(options, [f'cp -a {options.paths.build}/packages {ctx.docker_files}'])
    shutil.copytree(f'{options.paths.sncprovision}', f'{ctx.docker_files}/snc-provision', symlinks=True)
    arguments = {
        'BASE_IMAGE': base_image(ctx, options)
    }
    if options.get('raptordb_pro') == 'true' or not options.get('raptordb_pro'):
        arguments['RAPTORDB_PRO'] = 'true'
    image_name = 'jenkins-postgres'
    default_tag = 'tembo.dev-unstable'
    tag = options.get('version', default_tag)
    # Do not include arch in the image name for amd64 images.
    # This is the current contract for the glide IT test pipeline in Jenkins.
    override_name = image_name if options.get('architecture') == 'amd64' else ''

    return build(ctx, options, image_name, 'glideIT', arguments=arguments, override_version=tag, override_name=override_name)
 
@task
@cmdopts(CMDOPTS)
def create_licensed_rhel_base(options):
    """Consumes a license and creates the base rhel8 or rhel9 image with valid a subscription."""
    ctx = ContainerContext(options)

    if not options.operating_system.is_rhel():
        sys.exit(f'create licensed rhel base is not supported for {options.operating_system.name}')

    rhel_version = options.operating_system.rhel_version()
    LOG.info(f'Build rhel{rhel_version} base image with valid subscription')
    build(ctx, options, f'rhel{rhel_version}-base', f'rhel{rhel_version}-base', {}, 'latest')

@task
@cmdopts(CMDOPTS)
def create_benchmark(options):
    """Creates a benchmark container for the currently selected OS/host-database/build-type combination"""
    ctx = ContainerContext(options)

    LOG.info(f'Creating benchmark container for {ctx.operating_system}/{ctx.host_database}/{ctx.building_type}')

    create_product_files(ctx, options)
    build(ctx, options,
        f'{ctx.operating_system}-{ctx.host_database}-product-{ctx.building_type}', 'product',
        {'BASE_IMAGE': base_image(ctx, options)}
    )

@task
@cmdopts(CMDOPTS)
def create_all_developer(options):
    """(Re)builds all developer containers"""

    call_task('targets.all', args = ['container.create_developer'])

@task
@cmdopts(CMDOPTS)
def create_all_benchmark(options):
    """(Re)builds all benchmark containers"""

    call_task('targets.all', args = ['container.create_benchmark'])

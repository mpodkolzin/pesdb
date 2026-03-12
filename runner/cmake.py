import sys

from paver.easy import *
from git import Repo

import os
import glob
import instance
import logging
import datetime
import re
import shutil

from enums import BuildCleaning

LOG = logging.getLogger(__name__)

def git_build_number(options):
    if options['building']['package-buildid'] != 1:
        return options['building']['package-buildid']
    elif options.teamcity:
        # use origin/release/8.0 commit id as the reference as not all the branches are pulled
        return Repo(options.paths.dev).git.rev_list('--count', 'HEAD', '^f16dbb2b95f7c93f0a8dcd75a1755341c5dd4f7e')
    else:
        return options['building']['package-buildid']

def git_short_hash(options):
    if options.teamcity:
        return Repo(options.paths.dev).head.object.hexsha[:8]
    else: # if it's a devbuild use a constant git "hash"
        return "devbuild"

def git_full_hash(options):
    if options.teamcity:
        return Repo(options.paths.dev).head.object.hexsha
    else: # if it's a devbuild use a constant git "hash"
        return "devbuild"

def git_last_commit_msg(options):
    if options.teamcity:
        # escape special characters, since they cause shell command errors when commit message is put
        # into the cmake command:  cmake ... -DGIT_COMMIT_MSG="message"
        return re.sub(r'([\"\'\\])', r'\\\1', Repo(options.paths.dev).head.commit.message.partition('\n')[0])
    else: # if it's a devbuild use a constant git commit message
        return ""

def git_branch(options):
    if options.teamcity:
        return Repo(options.paths.dev).active_branch.name
    else: # if it's a devbuild use a constant git branch name
        return ""

def cleanup(options):
    paths = options.paths
    operating_system = options.operating_system.name.lower()
    host_database = options.host_database.name.lower()
    cmake_cmd_file_glob =  f'{paths.runner}/cmake_{operating_system}_{host_database}_*'

    if options.build_cleaning & BuildCleaning.build:
        LOG.info("Cleaning build cmake files for all build types")
        # cleanup cmake cmd too otherwise we won't run cmake
        for cmake_file in glob.glob(cmake_cmd_file_glob):
            os.remove(cmake_file)

    if options.build_cleaning & BuildCleaning.artifacts:
        # delete the data using root in the container so we can bypass most security bullshit
        LOG.info("Cleaning artifacts")
        instance.execute_root(options, ['rm -rf artifacts'], options.paths.runner)

    # make sure the instance is gone so we can actually delete all data.
    instance.stop()
    instance.cleanup_volumes(options)

    # Make sure we are only called once
    options.build_cleaning = BuildCleaning.nothing

def setup(options):
    paths = options.paths
    # build dir is not mapped to outside, use runner dir itself
    operating_system = options.operating_system.name.lower()
    host_database = options.host_database.name.lower()
    building_type = options.building_type.name.lower()

    cmake_cmd_file =  f'{paths.runner}/cmake_{operating_system}_{host_database}_{building_type}'

    if options.build_cleaning != BuildCleaning.nothing:
        cleanup(options)

    defines = options.get('defines') or {}

    defines['CMAKE_BUILD_TYPE'] = options.building_type.name
    defines['RUN_CLANG_TIDY'] = options['building']['clang-tidy']
    defines['GIT_HASH'] = git_short_hash(options)
    defines['GIT_FULL_HASH'] = git_full_hash(options)
    defines['GIT_COMMIT_MSG'] = git_last_commit_msg(options)
    defines['GIT_BRANCH'] = git_branch(options)

    defines['BUILD_METADATA'] = options['building']['metadata']
    defines['S64DA_PRODUCT_VERSION'] = options['building']['product-version']
    defines['S64DA_PG_CVE_LEVEL'] = options['building']['pg-cve-level']
    defines['S64DA_BUILD_NUMBER'] = git_build_number(options)
    defines['S64DA_PACKAGE_POSTFIX'] = options['building']['postfix'] or ''
    defines['VALGRIND_ATTACHED'] = 1 if options['building']['use-valgrind'] else 0

    package_kind = options['building']['package-kind']
    if package_kind is None:
        package_kind = 'database'

    # Check package kind is either 'database' or 'modlog-reader'
    if package_kind not in ['database', 'modlog_reader']:
        LOG.error("package-kind should either be 'database' or 'modlog_reader' but is '%s'", package_kind)
        sys.exit(-1)
    defines['PACKAGE_KIND'] = package_kind

    os.makedirs(paths.build, exist_ok=True)

    cmake_cmd = f'cmake {paths.dev} "-GNinja"'

    # sort all defines so that the command is more "stable" no matter the call-stack
    cmake_cmd+= ' ' + ' '.join(sorted([f'-D{k}="{v}"' for k, v in defines.items()]))

    # We cache the CMake command line and don't re-configure CMake if it matches
    # the previous invocation. Doing so would not be harmful, but would produce
    # the same output multiple times -- once for each time we are called.
    cmake_cache = f'{paths.build}/CMakeCache.txt'

    cmd_file_exists = os.path.isfile(cmake_cmd_file)
    cmd_is_same = cmd_file_exists and (open(cmake_cmd_file, 'r').read() == cmake_cmd)

    if not cmd_file_exists:
        print('========== Running CMake configuration for the first time ==========')
        print(cmake_cmd)
        print('==========')
    elif not cmd_is_same:
        print('========== Re-running CMake configuration as command line has changed ==========')
        print("NEW: %s" % (cmake_cmd))
        print("OLD: %s" % (open(cmake_cmd_file, 'r').read()))
        print('==========')
    else:
        return

    docker_cmd = [cmake_cmd]
    if not cmd_is_same:
        if cmd_file_exists:
            os.remove(cmake_cmd_file)
        docker_cmd.insert(0, 'rm -f {}'.format(cmake_cache))

    instance.execute(options, docker_cmd, options.paths.build)

    # CMake ran correctly, store the command line.
    open(cmake_cmd_file, 'w').write(cmake_cmd)

def build(options, targets):
    setup(options)
    parallelism = options['building']['parallelism']
    continue_on_error = '-k0' if options['building']['clang-tidy'] else ''
    cmd = [ f"ninja -j{parallelism} {' '.join(targets)} {continue_on_error}"]
    return instance.execute(options, cmd, options.paths.build)

from paver.easy import *

import all_options
import instance
import logging
import shutil
import os
import sys
import re
import subprocess
from datetime import datetime,timedelta
from git import Repo
from git import RemoteProgress
from tqdm import tqdm
from enums import OperatingSystem
import filecmp
from test import junit
import glob
from paver.options import Bunch
from enums import *

LOG = logging.getLogger(__name__)

def redirect_output_to(filename):
    file=open(filename, 'w')
    os.dup2(file.fileno(), 1)  # Redirect stdout (fd 1)
    os.dup2(file.fileno(), 2)  # Redirect stderr (fd 2)
    sys.stdout = file
    sys.stderr = file

@task
@consume_args
def validate_plugin_files(options, args):
    # Matches only file names >= version 23.1. Previously, we didn't have the requirement that
    # extension plugin files for minor version upgrades must be empty.
    pattern = re.compile("swarm64da--\\d+.\\d+--(2[3-9]|[3-9]\\d).[1-9]+.sql.tpl")
    invalid_files = []
    valid_files = ['swarm64da--23.0--23.1.sql.tpl',
                   'swarm64da--23.2--23.3.sql.tpl',
                   'swarm64da--23.4--23.5.sql.tpl',
                   'swarm64da--24.0--24.1.sql.tpl',
                   'swarm64da--29.1--30.1.sql.tpl'
                  ]
    dir = f'{options.paths.dev}/db/plugins/swarm64da/'

    for file_name in os.listdir(dir):
        if file_name not in valid_files and pattern.match(file_name):
            with open(os.path.join(dir, file_name)) as file:
                for line in file:
                    if not line.startswith('--') and not line.startswith('\\echo'):
                        invalid_files.append(file_name)
                        break

    if invalid_files:
        print(f'Invalid plugin file(s) found:')
        for file_name in invalid_files:
            print(f'  {file_name}')
        sys.exit(1)


@task
@consume_args
def clang_format_pre_commit_hook(options, args):
    if 'ports' in options.docker:
        options.docker.pop('ports')
    options.no_tty = True

    commands = []
    auto_apply = bool(options.get('building', {}).get('auto-apply-clang-format'))
    formatted = False

    for file in args:
        formatted_file = f"{file}.formatted"
        commands.append(f'clang-format "{file}" > {formatted_file}')

    if commands:
        instance.execute(options, commands, options.paths.dev)

    fix_commands = []
    for file in args:
        formatted_file = f"{file}.formatted"
        if filecmp.cmp(file, formatted_file):
            os.remove(formatted_file)
        else:
            formatted = True
            print(f' - Formatted {file}')
            os.replace(formatted_file, file)
            if auto_apply:
                fix_commands.append(f'git add {file}')

    if fix_commands:
        instance.execute(options, fix_commands, options.paths.dev)

    # Pre-commit will automatically fail if any changes were done in the working directory.
    # However, if this task is invoked from elsewhere, we need to exit with a non-zero code still.
    if formatted and not auto_apply:
        exit(1)


@task
@consume_args
def fix_includes_pre_commit_hook(options, args):
    if 'ports' in options.docker:
        options.docker.pop('ports')
    options.no_tty = True

    auto_apply = bool(options.get('building', {}).get('auto-apply-clang-format'))
    include_changed = False

    for file_name in args:
        if file_name.startswith('third_party') or file_name.startswith('postgres'): continue

        with open(file_name, mode='rb') as f:
            lines = f.readlines()

        new_lines = []
        for line in lines:
            if b'#include' in line and b'"' in line:
                line = line.replace(b'"', b'<', 1)
                line = line.replace(b'"', b'>', 1)
            new_lines.append(line)

        if lines != new_lines:
            include_changed = True
            with open(file_name, mode='wb') as f:
                for line in new_lines:
                    f.write(line)
            if auto_apply:
                instance.execute(options, [f'git add {file_name}'], options.paths.dev)

    # Pre-commit will automatically fail if any changes were done in the working directory.
    # However, if this task is invoked from elsewhere, we need to exit with a non-zero code still.
    if include_changed and not auto_apply:
        exit(1)

@task
@cmdopts([('port=', '', 'clangd port to use')])
def clangd(options):
    port = options.get('port', 5400)
    options['docker']['ports'] = { f'{port}/tcp': port }
    # refuse to clean anything, otherwise all our logging goes beserk
    options['building']['clean'] = 'nothing'
    options['building']['type'] = 'Debug'
    all_options.apply_changes(options)

    redirect_output_to(f'{options.paths.artifacts}/log/clangd-runner.log')

    # start container first, so that the port is forwarded
    instance._start(options)
    # build the plugin (as we need the PG compile commands too, which means we need to compile and extract them...)
    # however ignore any errors and move on.
    try:
        call_task('build.plugin')
    except:
        pass

    instance.execute(options, [f"/swarm/clangd.py {port}"], options.paths.dev)

@task
def print_tabcompletion(options):
    all_tasks = environment.get_tasks()
    our_task_names = []
    for task in all_tasks:
        if not task.name.startswith('paver'):
            our_task_names.append(task.name)

    print(' '.join(our_task_names))

# Can be used in scripts
# for example:
#       alias view_log='less $(runner get_logfile)'
#       alias truncate_log='rm $(runner get_logfile)'
# Useful when switching release/debug or os targets
@task
def get_logfile(options):
    print(f'{options.paths.artifacts}/log/postgresql.log')

@task
@consume_args
@needs('build.all')
def modlog_reader(options, args):
    """Build and runs the modlog_reader tool.
    """
    tool_dir = f'{options.paths.build}/tools/modlog_reader'
    all_args = ' '.join(args)
    instance.execute(options,
                [
                    f'{tool_dir}/modlog_reader {all_args}'
                ])

class GitProgressBar(RemoteProgress):
    def __init__(self, description, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pbar = tqdm(total=100.0, unit='%', desc=description)

    def update(self, op_code, cur_count, max_count=None, message=''):
        self.pbar.n = cur_count * 100.0 / max_count
        self.pbar.refresh()

    def __del__(self):
        self.pbar.update(100.0)
        self.pbar.close()

# Operate repo outside of the docker so you should have all credentials setup
def clone_snc_provision(options):
    """Clone the snc_provision repository"""
    snc_provision_url = options.get('snc-provision')['url']
    snc_provision_path = options.paths.sncprovision
    if os.path.exists(snc_provision_path + '/.git'):
        return Repo(snc_provision_path)
    # The path could be created as empty folders by non snow runs previously become we map it to the container.
    # Delete it to be able to do clone.
    if os.path.exists(snc_provision_path):
        shutil.rmtree(snc_provision_path)
    return Repo.clone_from(snc_provision_url, snc_provision_path, depth=1, progress=GitProgressBar("Cloning snc-provision"))

@task
def ensure_snc_provision_branch(options):
    # On using snc-provision, override the arch to amd64 as it only supports amd64
    options.architecture = Architecture.amd64
    all_options.apply_changes(options)

    repo = clone_snc_provision(options)
    branch = options.get('snc-provision')['branch']

    # See STRY61351685
    # A different snc provision branch is needed for centos7 because of ruby version 
    if options.operating_system == OperatingSystem.centos_7:
        branch += '-el7'
        LOG.info(f"Updating snc-provision-branch for centos-7. {branch}")

    if not branch:
        sys.exit("snc-provision branch must not be empty")
    is_local_branch = branch in [branch.name for branch in repo.branches]
    # if the branch doesn't exist locally, fetch it from remote and checkout
    if not is_local_branch:
        repo.remotes.origin.fetch(f'{branch}:{branch}', depth=1, force=True, progress=GitProgressBar(f"Fetching branch {branch}"))
    if repo.active_branch.name != branch:
        repo.git.checkout(branch)
    if not is_local_branch:
        return
    if not options.get('snc-provision')['sync']:
        LOG.info("Skip syncing branch to remote")
        return
    # if the branch exists locally, checkout and fetch the remote content, then force reset to it
    repo.remotes.origin.fetch(f'{branch}:remotes/origin/{branch}', depth=1, force=True, progress=GitProgressBar(f"Fetching branch {branch}"))
    repo.git.reset('--hard', f'origin/{branch}')

@task
@cmdopts([('zboot_log=', '', 'zboot logfile path'), ('out_file=', '', 'output destination')])
def parse_zboot_log(options):
    logfile = options.get('zboot_log')
    if not logfile:
        sys.exit('Can not find zboot log file')

    log_patterns_seq = ['zboot_start', 'zboot_end', 'first_text_index']
    log_patterns = {
        'zboot_start': 'ZBOOT: zboot_cold_instance.js start',
        'zboot_end': 'ZBOOT: zboot_cold_instance.js end',
        'first_text_index': 'Completed: text index events process in'
    }
    log_patterns_idx = 0
    log_patterns_line = {}
    plugin_loaded_pattern = 'Finished Loading plugin:'
    plugin_loaded_lines = []
    plugins_loaded = 0
    with open(logfile, 'r') as file:
        for line in file:
            if (log_patterns_idx >= len(log_patterns_seq)):
                break
            log_pattern_key = log_patterns_seq[log_patterns_idx]
            if (log_patterns[log_pattern_key] in line):
                log_patterns_line[log_pattern_key] = line
                log_patterns_idx += 1
            if (plugin_loaded_pattern in line):
                plugin_loaded_lines.append(line)
                plugins_loaded += 1

    if log_patterns_idx < len(log_patterns_seq):
        sys.exit('Can not find all needed patterns')

    def parse_time(datetimestr):
        return datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S (%f)')

    log_time_prefix_len = len('2024-09-19 16:50:01 (450)')
    zboot_start_time = parse_time(log_patterns_line['zboot_start'][:log_time_prefix_len])
    zboot_end_time = parse_time(log_patterns_line['zboot_end'][:log_time_prefix_len])

    plugin_loaded_time_re = re.compile(f'.*{plugin_loaded_pattern} (.*) \\[(\\d+)ms].*')
    plugin_loaded_time = 0
    for plugin_loaded_line in plugin_loaded_lines:
        t = plugin_loaded_time_re.match(plugin_loaded_line).group(2)
        plugin_loaded_time += int(t)

    text_index_time_re = re.compile(".*Completed: text index events process in (.*), next occurrence is.*")
    text_index_time = text_index_time_re.match(log_patterns_line['first_text_index']).group(1)

    def write_summary(file):
        print('Zboot log parse summary:', file=file)
        print(f'zboot time: {zboot_end_time - zboot_start_time}', file=file)
        print(f'plugins loaded: {plugins_loaded} in {timedelta(milliseconds=plugin_loaded_time)}', file=file)
        print(f'text indexing took {text_index_time}', file=file)

    outfile = options.get('out_file')
    if outfile:
        with open(outfile, "w") as f: write_summary(f)
    else:
        write_summary(sys.stdout)

@task
@consume_args
def generate_user_doc(options):
    """Generate the PDF documentation for self hosted customers."""
    if (options['operating-system']) == OperatingSystem.centos_7.name:
        sys.exit("generate_user_doc is not available in CentOS image. Use either Ubuntu20.04 or RHEL8")
    source_dir = f'{options.paths.dev}/db/doc/user'
    instance.execute(
        options,
        ['./generate-self-hosted-documentation.sh ' + options['building']['product-version'] + ' ' + f'{source_dir}/fonts'],
        source_dir)
    target_dir = f'{options.paths.artifacts}/documentation'
    instance.execute(
        options,
        [f'mkdir -p {target_dir} && mv {source_dir}/raptordb-pro-documentation-rel-*.pdf {target_dir}'])
    print(f"Documentation generated in {target_dir}")

@task
@consume_args
def junit_spotless_format(options, args):
    if 'ports' in options.docker:
        options.docker.pop('ports')
    options.no_tty = True

    auto_apply = bool(options.get('building', {}).get('auto-apply-junit-format'))
    junit_prefix = 'db/tests/integration/junit/' # pre-commit hook always uses full repo path.

    # Run the junit task to format the files.
    options.junit = Bunch(cmd='spotlessApply', spotlessFiles='')
    if args:
        for file in args:
            if file.startswith(junit_prefix):
                file = file[len(junit_prefix):]  # Remove the prefix.
            options.junit['spotlessFiles'] += f'{file},'
        options.junit['spotlessFiles'] = options.junit['spotlessFiles'][:-1]  # Remove trailing comma
    call_task('junit')

    commands = []
    formatted = False

    junit_src_dir = f'{options.paths.dev}/{junit_prefix}'
    junit_bld_dir = f'{options.paths.runner}/junit/'

    # Diff the files in the junit build/project directory with the original files and git add the ones that changed.
    for bld_file in glob.glob(f'{junit_bld_dir}src/test/java/**/*.java', recursive=True):
        src_file = bld_file.replace(f'{junit_bld_dir}', f'{junit_src_dir}')
        if not filecmp.cmp(bld_file, src_file):
            formatted = True
            print(f' - Reformatted {src_file}')
            commands.append(f'cp {bld_file} {src_file}')
            if auto_apply:
                commands.append(f'git add {src_file}')

    if commands:
        instance.execute(options, commands, options.paths.dev)

    # Pre-commit will automatically fail if any changes were done in the working directory.
    # However, if this task is invoked from elsewhere, we need to exit with a non-zero code still.
    if formatted and not auto_apply:
        exit(1)

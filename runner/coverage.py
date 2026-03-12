from paver.easy import *

import instance
import all_options
import atexit
import re
import os
import sys

from enums import *


@task
def report(options):
    """Create the coverage report after running programs for which this is desired."""

    option1 = "--rc lcov_branch_coverage=1"
    option2 = "--gcov-tool gcov-11"
    included_postgres_files = "'*/postgres/*'"
    all_included_files = "'*/db/*' " + included_postgres_files
    # a hack for using lcov. A wrapper for 'llvm-cov gcov' command so that "gcov-tool" can be used as --gcov-tool argument can't handle the space
    instance.execute_root(
        options,
        [   "echo '#!/bin/bash' > /usr/bin/gcov_for_clang.sh  ",
            '''echo 'exec llvm-cov gcov -u "$@"' >> /usr/bin/gcov_for_clang.sh ''',
            '''chmod +x /usr/bin/gcov_for_clang.sh ''',
        ])

    instance.execute_root(
        options,
        [   "lcov --gcov-tool 'gcov_for_clang.sh' {1}  --directory . --capture --output-file {0}.info && "
            "lcov --extract {0}.info {3} --output-file {0}.info.our_code {1} {2} && "
            "lcov --remove {0}.info.our_code 'tests/*' '*/unit/*' '*/devtoolset*/*' '*/catch.hpp' '*/third_party/*' '*/contrib/*' '*/glide/*' --output-file {0}.info.cleaned {1} {2} && "
            "genhtml --highlight --legend -ignore-errors source -o {0} {0}.info.cleaned {1} && "
            f'''chown -R host_user {options.paths.artifacts}/coverage_report && '''
            'echo "Open {0}/index.html in your browser to view the coverage report."'.
            format(f"{options.paths.artifacts}/coverage_report", option1, option2, all_included_files)
        ], options.paths.build)

@task
@consume_args
def run(options, args):
    """Run the task(s) given and creates a coverage report for them"""

    options['building']['type'] = 'Coverage'
    all_options.apply_changes(options)

    # clear all previous gcov(r)/lcov results
    instance.execute(options, ["find . -type f -name '*.gcda' -o -name '*.gcov' -delete"], options.paths.build)

    for task in args:
        call_task(task, args={})

    call_task('coverage.report')


@task
def init(options):
    """Run the coverage initialization, used to run a set of tests or a specific test in a format like:
    coverage.init test.<test_type> --test <test_name> test.<test_type> --test <test_name> coverage.report """
    options['building']['type'] = 'Coverage'
    all_options.apply_changes(options)

    # clear all previous gcov(r)/lcov results
    instance.execute(options, ["find . -type f -name '*.gcda' -delete"], options.paths.build)


@task
@needs(['build.plugin'])
@cmdopts([('source_path_pattern=', '', 'if not in teamcity, set the source path pattern to be replaced'),
    ('coverage_reports_dir=', '', 'directory where the coverage reports are stored. If not provided, {build_dir}/multiple_coverage_reports/ assumed')])
def merge_lcov_reports(options):
    """Merge all the lcov reports in the coverage_reports_dir directory and generate an html report. """

    options['building']['clean'] = BuildCleaning.nothing

    source_path_pattern = options.get('source_path_pattern', '/mnt/data/ec2-user/agent-.*/work/runner/dev/')
    new_source_path = f'''{options.paths.dev}/'''
    trace_files = []
    root_build_dir = options.paths.runner

    coverage_reports_dir = options.get('coverage_reports_dir', f'''{root_build_dir}/multiple_coverage_reports''')
    merged_coverage_report_info_path = f'{coverage_reports_dir}/merged.info'
    # check if the directory exists
    if not os.path.exists(coverage_reports_dir):
        print(f"Directory {coverage_reports_dir} does not exist.")
        return

    cov_options = "--rc lcov_branch_coverage=1"

    # Walk through the directory and subdirectories
    for root, _, files in os.walk(coverage_reports_dir):
        for file in files:
            if file.endswith('.info.cleaned'):
                instance.execute_root(options, [f'''sed -i 's#{source_path_pattern}#{new_source_path}#g' {os.path.join(root, file)}'''])
                trace_files.append(os.path.join(root, file))

    # Check if there are any info files to merge
    if not trace_files:
        print(f"No *.info.cleaned files found in coverage directory: {coverage_reports_dir} ")
        return

    # Construct the lcov command as a string
    lcov_command = f'''lcov --add-tracefile {trace_files[0]}  '''
    for info_file in trace_files[1:]:
        lcov_command += f" -a {info_file}"
    lcov_command += f" -o {merged_coverage_report_info_path} {cov_options}"
    
    genhtml_opts = '--highlight --legend -ignore-errors source '
    genhtml_command = f'''genhtml {genhtml_opts} -o {options.paths.artifacts} {merged_coverage_report_info_path} {cov_options}'''

    instance.execute_root(
        options,
        [   f'''{lcov_command} ''',
            f'''{genhtml_command}''',
            f'''echo "Open {coverage_reports_dir}/index.html in your browser to view the coverage report."''',
            f'''chown -R host_user {coverage_reports_dir}'''
        ], options.paths.build)
        
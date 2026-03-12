from ast import pattern
from jinja2 import Environment, FileSystemLoader
import logging
import glob
import os
import shutil
import time
from datetime import datetime
import container
import all_options
from instance import *
from enums import *
from paver.easy import *
import yaml
import subprocess
from mcp_runner.decorators import *
from utils import *

LOG = logging.getLogger(__name__)

def remove_tests(test_list, elements_to_remove):
   return [x for x in test_list if x not in elements_to_remove]

def list_matching_tests(pattern, full_path=False, recursive=False):
    if full_path:
        return sorted(glob.glob(pattern, recursive=recursive))
    else:
        return sorted([os.path.basename(os.path.splitext(x)[0]) for x in glob.glob(pattern, recursive=recursive)])

# Global var to store default product extensions.
prod_extensions = '''--load-extension=btree_gin  --load-extension=pg_buffercache --load-extension=pg_stat_statements --load-extension=pg_trgm '''

PGTAP_OPTS = [
    ('test=', 't', 'filter tests by substring. May match multiple tests. Do not include file extension.'),
    ('disable_parallelism', '', 'disable parallel execution of the tests'),
    ('details', '', 'show detailed output about failed tests'),
    ('test_dir=', '', 'test directory to use (default=tap)'),
    ('test_pattern=', '', 'test pattern for complex matching'),
    ('setup_replication', 'r', 'Setup replication (primary, standby, and read-replica) before running tests. Target tests to primary server.'),
    ('config', '', 'postgres server configuration to be used')
]

# Wrapper for call_task that will suppress the non-zero return code.
def call_task_suppress_exception(task_name):
    try:
        call_task(task_name)
    except:
        pass

def pgregress_print_teamcity_status(options, out_dir, test_files):
    if options.teamcity:
        failed_tests = []
        regress_out = f'{out_dir}/regression.out'
        if os.path.exists(regress_out):
            with open(regress_out) as file:
                for line in file:
                    if line.startswith('test'):
                        line = line[4:]
                    parts = line.strip().split()
                    test = parts[0]
                    status = parts[2]
                    if status == "FAILED":
                        failed_tests.append(test)

        for test_name in test_files:
            print(f"##teamcity[testStarted name='{test_name}']")
            if test_name in failed_tests:
                print(f"##teamcity[testFailed name='{test_name}']")
            print(f"##teamcity[testFinished name='{test_name}']")


@task
@consume_args
@needs(['build.plugin'])
def coverage_all(options):
    """Run all tests for coverage. All test failures are suppressed."""
    # We use the test suite to understand the overall test code coverage. We use call_task_suppress_exception
    # to suppress the exception of any failed test as the purpose of this task is to get the overall test coverage.

    # Tasks are called instead of using @needs so that we can control if the build directory cleaned up in between.
    # We set the clean to `False` so that even when clean=True no cleanup happens after initial build and
    # it will preserve coverage files when for example unit tests are run after integration tests.

    # We need to execute `pgtap_oom` first, because for that test we restrict the available RAM for the container to
    # 2GB. This restriction fails if the container has used more RAM than 2GB in the previous tests.
    options['building']['clean'] = BuildCleaning.nothing
    # We need to run this test first because one of the rest tests has some leftovers that cause it to fail.
    # skipping the test report for these tests
    options['skip_test_report'] = True

    call_task_suppress_exception('test.system')
    call_task_suppress_exception('test.pgtap_oom')
    call_task_suppress_exception('test.pgregress')
    call_task_suppress_exception('test.isolation')
    call_task_suppress_exception('test.pgtap_serial_all')
    call_task_suppress_exception('test.pgtap_parallel')
    call_task_suppress_exception('test.psql_regress_all')
    call_task_suppress_exception('test.psql_isolation_all')
    call_task_suppress_exception('test.script_of_doom')
    call_task_suppress_exception('test.resource_leaks')
    call_task_suppress_exception('test.obs_counters')
    call_task_suppress_exception('test.junit')
    call_task_suppress_exception('test.unit')
    call_task_suppress_exception('test.product_lifecycle')
    call_task_suppress_exception('test.modlog_reader_test')
    call_task_suppress_exception('test.modlog_reader_unit')
    # Contrib tests add changes to postgres source directory so running them last to avoid interfering with other tests.
    call_task_suppress_exception('test.psql_contrib_all')


@task
@consume_args
@needs(['build.plugin'])
def coverage_pr_suite(options):
    """Run PR tests for coverage. All test failures are suppressed."""
    # This subset of tests is to run coverage reporting on a PR and to track if we have sufficient coverage for the new features.
    # We use call_task_suppress_exception to suppress the exception of any failed test as the purpose of this task is to
    # get the overall test coverage and we don't want to fail the build if a test fails due to flakiness.

    # Tasks are called instead of using @needs so that we can control if the build directory cleaned up in between.
    # We set the clean to `False` so that even when clean=True no cleanup happens after initial build and it will
    # preserve coverage files when for example unit tests are run after integration tests.

    options['building']['clean'] = BuildCleaning.nothing
    # Skipping the test report for these tests as primary goal is to get the coverage.
    options['skip_test_report'] = True

    call_task('coverage_pr_suite_pt1')
    call_task('coverage_pr_suite_pt2')
    call_task('coverage_pr_suite_pt3')
    call_task('coverage_pr_suite_pt4')


# 3 subsets of tests to run coverage reporting on a PR and to track if we have sufficient coverage for the new features.
# We split PR tests into 3 subsets based on how long TC jobs are and to be able to run them in parallel.
@task
@consume_args
@needs(['build.plugin'])
def coverage_pr_suite_pt1(options):
    """Run PR (part1) tests for coverage. All test failures are suppressed."""
    # Skipping the test report for these tests as primary goal is to get the coverage.
    options['skip_test_report'] = True
    options['building']['clean'] = BuildCleaning.nothing
    call_task_suppress_exception('test.pgtap_serial_all')

@task
@consume_args
@needs(['build.plugin'])
def coverage_pr_suite_pt2(options):
    """Run PR (part2) tests for coverage. All test failures are suppressed."""
    # Skipping the test report for these tests as primary goal is to get the coverage.
    options['skip_test_report'] = True
    options['building']['clean'] = BuildCleaning.nothing
    call_task_suppress_exception('test.pgtap_parallel')

@task
@consume_args
@needs(['build.plugin'])
def coverage_pr_suite_pt3(options):
    """Run PR (part3) tests for coverage. All test failures are suppressed."""

    options['building']['clean'] = BuildCleaning.nothing
    # Skipping the test report for these tests as primary goal is to get the coverage.
    options['skip_test_report'] = True
    call_task_suppress_exception('test.system')
    call_task_suppress_exception('test.pgregress')

@task
@consume_args
@needs(['build.plugin'])
def coverage_pr_suite_pt4(options):
    """Run PR (part4) tests for coverage. All test failures are suppressed."""

    options['building']['clean'] = BuildCleaning.nothing
    # Skipping the test report for these tests as primary goal is to get the coverage.
    options['skip_test_report'] = True
    call_task_suppress_exception('test.psql_regress_all')
    call_task_suppress_exception('test.psql_regress_crash_test')
    call_task_suppress_exception('test.junit')
    call_task_suppress_exception('test.unit')
    call_task_suppress_exception('test.modlog_reader_test')
    call_task_suppress_exception('test.modlog_reader_unit')
    call_task_suppress_exception('test.pg_hint_native')
    call_task_suppress_exception('test.pgaudit_native')

@task
# This task is aimed to run a subset of valgrind tests on TC.
@cmdopts(PGTAP_OPTS)
def teamcity_valgrind_pgtap(options):
    """Run valgrind with pgtap parallel tests."""
    # Force use-valgrind.
    options['building']['use-valgrind'] = True
    options['building']['type'] = BuildingType.Debug.name
    options['operating-system'] = OperatingSystem.rhel_8.name

    # Workaround to allow running valgrind tests on TC. No need to reset it back as we don't run other tests after this.
    options['disable_cap'] = 'true'
    call_task('build.set_capabilities')

    # As it will take too long (>24 hours) to run all the tests, we run a sub-set from them regularly.
    # Exclude list contains tests which are either a variation of the same test scenario or might fail due to timeouts.
    excluded_tests = [
                        '_join_types',
                        '1021_01_join_datatypes',
                        '1021_02_join_datatypes',
                        '0201_01_columnstore_datatypes',
                        '0201_02_columnstore_datatypes',
                        '0201_03_columnstore_datatypes',
                        '7006', # perf analysis will always fail due to timeouts
                        '1055_join_selectivity', # will fail due to timeouts
                        '7026_cte_perf',
                        'performance_analysis',
                        '01_tuple_converter',
                        '02_tuple_converter',
                        '1043_sql_compat', # disabling for now (till SDB-11778) as due to big number of permutations the tests take > 4.5 hours
                        '_sncvarchar_like_to_btree_range', # will fail due to timeouts
                        '1059_skip_scan_enforce_jit', # causes statement timeout
                        '1059_skip_scan_group_by', # 6 hours
                        '1059_2_skip_scan_func', # 4 hours
                        '1059_1_skip_scan_func', # 3 hours
                        '1061_btree', # causes significantly longer runtimes
                        '1062_btree_not_equal_data_types', # causes statement timeout
                        '1059_skip_scan_type_index_permutations', # causes statement timeout
                        '1036_AND_OR_to_ANY_ALL_queries', # causes statement timeout
                        '1071_ptc_under_or', # causes statement timeout
                        '1055_in_selectivity', # too long to run
                        '1005_2', # too long to run
                        '1063', # too long to run
                        '1073_', # too long to run
                        '1060', # too long to run
                        '1072_equal_unique_fastpath', # tests which check 'time' can't be reliably run in valgrind
                        '_0_plan_choice', # too long to run
                        '1035', # too long to run
                        '1057_hints', # too long to run (3 hours)
                        '1038', # too long to run (3 hours)
                        '1002_0_join_rescan', # too long to run (4 hours)
                        '1100', # too long to run
                        '1006_0_rewriting_ja_pattern' # too long to run (5 hours)
                  ]

    # Constructing glob extended pattern in a format like: @(!(*join_types*|*060_0_performance_analysis_explain*)).
    pattern = options.get('test', None)
    if not pattern:
        exclude_pattern = '*|*'.join(excluded_tests)
        options['test_pattern'] = f'@(!(*{exclude_pattern}*))'
        options['test_dir'] = options.get('test_dir', 'tap/parallel')

    options['config'] = 'integration-parallel'
    # Skipping the test report for these tests as primary goal is to get the valgrind analysis.
    options['skip_test_report'] = True
    call_task_suppress_exception('test.pgtap')

    valgrind_dir = f'{options.paths.artifacts}/log/valgrind'
    valgrind_errors_present = False
    for filename in glob.iglob(valgrind_dir + '**/**/*.log', recursive=True):
        execute(options,
            [f'''sed -i '/WARNING: valgrind ignores/d' {filename}'''])
        if os.path.getsize(filename) == 0:
            os.remove(filename)
        else:
            valgrind_errors_present = True

    if valgrind_errors_present:
        raise Exception('Test failed. Non-empty valgrind logs')


@task
@consume_args
@needs(['pgregress', 'pgtap_serial_all', 'pgtap_parallel', 'pgtap_oom', 'isolation'])
def integration(options):
    """Run all integration type tests (both pg_regress, pgtap and oom)."""

@task
@cmdopts([('test=', 't', 'filter tests by substring May match multiple tests. Do not include file extension.')])
@needs(['build.plugin'])
@mcp_enabled
def isolation(options):
    """Run the Swarm64 DA PostgreSQL plugin isolation tests."""

    isolation_dir = f'{options.paths.dev}/db/tests/isolation'
    out_dir = f'{options.paths.artifacts}/isolation'
    sched_file = f'{out_dir}/isolation_schedule'

    # Create schedule file.
    pattern = options.get('test', '*')
    spec_files = list_matching_tests(f'{isolation_dir}/specs/*{pattern}*.spec')

    if not spec_files:
        print("No matching test found for pattern")
        return

    exclude = [ 't014_concurrent_redundant_updates',
                't015_5_inplace_update_concurrent',
                't006_columnstore_concurrent' ]
    if options['building']['type'] == BuildingType.Release.name:
        spec_files = remove_tests(spec_files, exclude)

    print(spec_files)

    os.makedirs(out_dir, exist_ok=True)
    with open(sched_file, 'w') as file:
        for f in spec_files:
            if options['building']['type'] != BuildingType.Release.name or not f.endswith("_nonrelease"):
                file.write(f'test: {f}\n')
    options['config'] = 'isolation'
    call_task('db.start_server')
    execute(options,
            [f'$PG_BIN_DIR/pg_isolation_regress {prod_extensions}'
             f'--load-extension=swarm64da --schedule={sched_file} '
             f'--inputdir={isolation_dir} --outputdir={out_dir}'
            ])

@task
@cmdopts([('test=', 't', 'filter tests by substring May match multiple tests. Do not include file extension.'),
          ('test_dir=', '', 'test directory to use (default=pg_regress)')])
@needs(['build.plugin'])
def pgregress_generic(options):
    """Run the Swarm64 DA PostgreSQL plugin regression tests."""
    test_dir = options.get('test_dir', 'pg_regress')
    if test_dir == 'pg_regress':
        test_prefix = 't'
    else:
        test_prefix = ''

    regress_dir = f'{options.paths.dev}/db/tests/integration/{test_dir}/'
    out_dir = f'{options.paths.artifacts}/pgregress/'
    setup_replication = options.get('setup_replication', False)

    pattern = options.get('test', '*')
    # Tests in replication setup must have file name ending with 'replication'.
    replication_pattern = 'replication'
    test_files = options.get('test_files', None)
    if not test_files:
        if setup_replication:
            test_files = list_matching_tests(f'{regress_dir}/sql/{test_prefix}*{pattern}*{replication_pattern}.sql')
        else:
            test_files_all = list_matching_tests(f'{regress_dir}/sql/{test_prefix}*{pattern}*.sql')
            test_files = []
            for test_file in test_files_all:
                if '_replication' not in test_file:
                    test_files.append(test_file)
        files_to_execute = 'startup {} shutdown'.format(' '.join(test_files))
    else:
        files_to_execute = '{}'.format(' '.join(test_files))
    if not test_files:
        print("No matching test found for pattern")
        return

    if setup_replication:
        options['config'] = 'primary-integration'
        call_task('db.start_primary')
        options['config'] = 'standby-integration'
        call_task('db.start_standby')
        options['config'] = 'read-replica-integration'
        call_task('db.start_read_replica')
    else:
        options['config'] = options.get('config', 'integration')
        call_task('db.start_server')
    try:
        execute(
            options,
            [f'mkdir -p {out_dir}',
             f'$PG_BIN_DIR/pg_regress --dbname=contrib_regression {prod_extensions}'
             f'--inputdir={regress_dir} --outputdir={out_dir} '
              f'{files_to_execute}'
            ],
            regress_dir
        )
    finally:
        pgregress_print_teamcity_status(options, out_dir, test_files)

@task
@cmdopts([('test=', 't', 'filter tests by substring May match multiple tests. Do not include file extension.')])
@needs(['build.plugin'])
@mcp_enabled
def pg_regress_replication(options):
    """Run the Swarm64 DA PostgreSQL plugin regression tests in a replication setup"""
    options['setup_replication'] = True
    call_task('test.pgregress_generic')

@task
@cmdopts([('test=', 't', 'filter tests by substring May match multiple tests. Do not include file extension.')])
@needs(['build.plugin'])
@mcp_enabled
def pg_hint_native(options):
    """Run the pg_hint_plan native tests"""
    options['setup_replication'] = False
    options['test_dir'] = 'pg_hint'
    options['config'] = 'pg-hint-native'
    options['test_files'] = ['init', 'base_plan', 'hints_anywhere', 'pg_hint_plan', 'plpgsql',
                             'ut-init', 'ut-fdw', 'ut-A', 'ut-G', 'ut-J', 'ut-L', 'ut-R',
                             'ut-S', 'ut-T', 'ut-W', 'ut-fini']
    call_task('test.pgregress_generic')

@task
@needs(['build.plugin'])
@mcp_enabled
def pgaudit_native(options):
    """Run the pgaudit native test"""
    options['setup_replication'] = False
    options['test_dir'] = 'pgaudit'
    options['config'] = 'integration'
    options['test_files'] = ['pgaudit']
    call_task('test.pgregress_generic')

@task
@cmdopts([('test=', 't', 'filter tests by substring May match multiple tests. Do not include file extension.')])
@needs(['build.plugin'])
@mcp_enabled
def pgregress(options):
    """Run the Swarm64 DA PostgreSQL plugin full regression tests suite both in replication and non-replication setup"""
    options['setup_replication'] = True
    call_task('test.pgregress_generic')
    call_task('stop_standby')
    call_task('stop_read_replica')
    call_task('stop_primary')

    options['setup_replication'] = False
    options['config'] = 'integration'
    call_task('test.pgregress_generic')



POSTGRES_NATIVE_CONF = 'psql-tests-pg_native-semantics'
POSTGRES_SQL_COMPAT_CONF = 'psql-tests-sql_compat-semantics'
DEFAULT_PGREGRESS_NATIVE_TESTS_CONF = POSTGRES_SQL_COMPAT_CONF


def has_replication_tests(base_dir, file_pattern, pattern):
    want_tests = list_matching_tests(f'{base_dir}/{file_pattern}', True)
    repl_tests = list_matching_tests(f'{base_dir}/tap/replication/*{pattern}*.pg', True)

    return any(item in repl_tests for item in want_tests)


@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
def pgtap(options):
    """Run TAP integration tests from specified directory (default: all tap/ subdirectories).

    Use test_dir parameter to filter to a specific directory, or use convenience tasks:
    - pgtap_serial: tests in tap/serial/
    - pgtap_parallel: tests in tap/parallel/
    - pgtap_replication: tests in tap/replication/
    - pgtap_configuration: tests in tap/configuration/
    - pgtap_oom: tests in tap/oom/
    - pgtap_serial_columnstore: tests in tap/parallel_columnstore/
    """

    integration_dir = f'{options.paths.dev}/db/tests/integration'
    setup_replication = options.get('setup_replication', False)
    test_dir = options.get('test_dir', 'tap/**')
    test_pattern = options.get('test_pattern', None)
    pattern = options.get('test', '*')
    parallelism = options['building']['test-parallelism']
    if options.get('disable_parallelism'):
        parallelism = 1
    details_args = '-v ' if options.get('details') else ''
    tc_args = '--nocolor --formatter TAP::Formatter::File' if options.teamcity else ''
    if test_pattern:
        file_pattern = f'{test_dir}/{test_pattern}.pg'
    else:
        file_pattern = f'{test_dir}/*{pattern}*.pg'
    colorized = ' | sed -u s"/not ok/\x1b[1;31m&\x1b[0m/g"' if not options.teamcity else ''

    # test_pattern is used only for TC valgrind and it has the pattern format that won't work with the
    # list_matching_tests() anyway, so skip testing for replication setup mismatch when test_pattern is set.
    if not setup_replication and not test_pattern and has_replication_tests(integration_dir, file_pattern, pattern):
        print("Pattern matches replication test(s) but replication is not set up.",
              "Use --setup_replication option or use ./runner test.pgtap_replication -t *pattern*.",
              "Use ./runner test.pgtap_[parallel|serial|etc...] -t *pattern* to run other test(s)",
              sep=os.linesep);
        return

    if not list_matching_tests(f'{integration_dir}/{file_pattern}') and not test_pattern:
        print("No matching test found for pattern")
        return
    if setup_replication:
        options['config'] = 'primary-integration'
        call_task('db.start_primary')
        options['config'] = 'standby-integration'
        call_task('db.start_standby')
        options['config'] = 'read-replica-integration'
        call_task('db.start_read_replica')
    else:
        options['config'] = options.get('config', 'integration')
        call_task('db.start_server')
    artifacts_dir = f'{options.paths.artifacts}/tap'

    def gen_run_test_cmd(state):

        if state not in ['failed', 'save']:
            ValueError("Invalid value for 'state' parameter. Expected 'failed' or 'save'.")
        _file_pattern =  '' if state == 'failed' else f'{file_pattern}'
        generate_state_file = f'--state {state}' if options.teamcity else ''
        # We want to show diagnostics in case:
        # - tests are running in teamcity and the state is 'failed' (they are rerunning)
        # or we run with details (regardless where) as details will conflict with quiet mode.
        diagnostic_messages =  '' if (options.teamcity and state != 'failed') else ''

        # Propagate --details to SQL via a custom GUC so tap_override_functions.diag() can decide
        # whether to output diagnostics. We set PGOPTIONS only when details are requested.
        pgoptions_prefix = 'PGOPTIONS="-c mytap.details=on" ' if options.get('details') else ''
        cmd = (f'/bin/bash -O extglob -o pipefail -c \'{pgoptions_prefix}pg_prove {generate_state_file} --failures --recurse --trap --timer --nocount --merge '
            f'--normalize --color -j{parallelism} {details_args} {tc_args} {diagnostic_messages} --rules="par=tap/parallel/*" '
            f'--dbname pgtap {_file_pattern} {colorized} | tee {artifacts_dir}/tap.log\'')
        return cmd


    def tap_test_crashed():
        with open(f'{artifacts_dir}/tap.log', 'r') as file:
            for line in file:
                if 'Non-zero exit status' in line:
                    print('System may have crashed! The tests will not be re-run. Check postgresql.log for more information.', file=sys.stderr)
                    return True
            return False

    with open(f'{integration_dir}/tap/flaky_tests') as f:
        known_flaky_tests = f.read().splitlines()

    try:
        # Remove .prove file first to ensure nothing is left from previous run.
        if os.path.exists(f'{integration_dir}/.prove'):
            os.remove(f'{integration_dir}/.prove')

        execute(options,
            [   f'mkdir -p {artifacts_dir}',
                'psql -c "DROP DATABASE IF EXISTS pgtap_template_db WITH (FORCE)"',
                # To isolate test cases, each test will be executed in its own database which will be created
                # based on pgtap_template_db. Since the template database cannot have active connections during
                # cloning, we will create a second database which will act as the default database.
                'psql -c "CREATE DATABASE pgtap_template_db"',
                'psql pgtap_template_db -c "CREATE EXTENSION pgtap"',
                'psql -c "DROP DATABASE IF EXISTS pgtap WITH (FORCE)"',
                # override default tap functions
                f'psql pgtap_template_db -f {options.paths.dev}/db/tests/integration/tap/tap_override_functions.sql',
                'psql -c "CREATE DATABASE pgtap TEMPLATE pgtap_template_db"',
                # Create test_user role so that the tests could be run with non superuser.
                # Because tests are not properly cleaning up after themselves, leaving databases around,
                # we can't DROP ROLE (we'd have to first loop through each database doing DROP OWNED BY).
                # Instead we emulate the CREATE ROLE IF NOT EXISTS (which is not currently supported):
                'psql -c "DO \\$\\$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = \'test_user\') THEN CREATE ROLE test_user WITH LOGIN; END IF; END \\$\\$"',
                # Initiate bash to allow extended glob pattern to match excluded files
                f'''{gen_run_test_cmd('save')}''',
            ],
            integration_dir)
    except Exception as e:
        if options.teamcity:
            failed_tests = get_list_of_tap_failed_tests(f'{integration_dir}/.prove')
            print(f'List of failed tests: {failed_tests}', file=sys.stderr)
            # Rerun failed tests even if they are not flaky to get the detailed output.
            execute(options,
                [
                    f'''{gen_run_test_cmd('failed')}''',
                ],
                integration_dir)
            if tap_test_crashed() or set(failed_tests) > set(known_flaky_tests):
                raise e
        else:
            raise e
    finally:
        if options.teamcity and not options.get('skip_test_report', False):
            teamcity_report_test_status(f'{integration_dir}/.prove', known_flaky_tests)
            if os.path.exists(f'{integration_dir}/.prove'):
                os.remove(f'{integration_dir}/.prove')


# Can in theory be merged with below get_list_of_tap_failed_tests function, however, making it a separate function
# ensures we don't adversely affect current state.
def teamcity_report_test_status(prove_path: str, flaky_tests: list):
    try:
        with open(prove_path) as file:
            data = yaml.safe_load(file)
            tests = data['tests']
            for test_name, test_meta in tests.items():
                tc_test_name = test_name.rsplit('/', 1)[-1]
                tc_test_name = tc_test_name.replace('.pg', '').replace('/', '.')
                print(f"##teamcity[testStarted name='{tc_test_name}']")
                if test_meta.get('total_failures', 0) > 0:
                    if test_name in flaky_tests:
                        print(f"##teamcity[testIgnored name='{tc_test_name}']")
                    else:
                        print(f"##teamcity[testFailed name='{tc_test_name}']")
                else:
                        print(f"##teamcity[testFinished name='{tc_test_name}']")
    except Exception as e:
        print(f'Failed to report test status: {e}')


def get_list_of_tap_failed_tests(prove_path, target_dir=None, options=None):
    failed_tests = []
    if options:
        execute(options, [f'cp -r {options.paths.tests}/system/.prove {prove_path}'], workdir='/')
    with open(prove_path, 'r') as file:
        data = yaml.safe_load(file)
    tests = data['tests']
    for test_name, test_meta in tests.items():
        if test_meta.get('total_failures', 0) > 0:
            processed_test_name = test_name
            if target_dir:
                # Find the target directory in the path and extract from that point.
                parts = test_name.split('/')
                for i, part in enumerate(parts):
                    if part == target_dir:
                        # Extract the path starting from the target directory.
                        processed_test_name = '/'.join(parts[i:])
                        break
                failed_tests.append(processed_test_name)
    return failed_tests

@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
@mcp_enabled
def pgtap_replication(options):
    """Run TAP tests in tap/replication/ directory with primary/standby/read-replica setup.

    Use general 'pgtap' task with --setup_replication if you don't know which directory your test is in."""
    call_task('db.stop_server')
    options['test_dir'] = 'tap/replication'
    options['setup_replication'] = True
    call_task('test.pgtap')


@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
@mcp_enabled
def pgtap_oom(options):
    """Run TAP tests in tap/oom/ directory (memory allocation tests with 2GB container limit).

    Use general 'pgtap' task with test_dir='tap/oom' if you don't know which directory your test is in."""
    update_limitations(options, '2g')
    options['test_dir'] = 'tap/oom'
    options['config'] = 'integration'
    call_task('test.pgtap')
    update_limitations(options, -1)

@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
@mcp_enabled
def pgtap_parallel(options):
    """Run TAP tests in tap/parallel/ directory (safe for parallel execution).

    Use general 'pgtap' task with test_dir='tap/parallel' if you don't know which directory your test is in."""
    options['config'] = 'integration-parallel'
    options['test_dir'] = 'tap/parallel'
    call_task('test.pgtap')

@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
@mcp_enabled
def pgtap_configuration(options):
    """Run TAP tests in tap/configuration/ directory (default production configuration tests).

    Use general 'pgtap' task with test_dir='tap/configuration' if you don't know which directory your test is in."""
    options['config'] = 'dev'
    options['test_dir'] = 'tap/configuration'
    call_task('test.pgtap')


@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
@mcp_enabled
def pgtap_serial_columnstore(options):
    """Run TAP tests in tap/parallel_columnstore/ directory (columnstore tests run serially due to xmin visibility checks).

    Use general 'pgtap' task with test_dir='tap/parallel_columnstore' if you don't know which directory your test is in."""
    options['config'] = 'integration'
    # Due to columnstore visibility checks using xmin, it's better to run these tests in serial to avoid conflicts.
    # If a solution is found to relax the xmin visibility checks in SDB-10801, these testcases should be moved back
    # to pgtap_parallel.
    options['test_dir'] = 'tap/parallel_columnstore'
    call_task('test.pgtap')



@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
@mcp_enabled
def pgtap_serial(options):
    """Run TAP tests in tap/serial/ directory (tests that must run serially).

    Use general 'pgtap' task with test_dir='tap/serial' if you don't know which directory your test is in."""
    options['config'] = 'integration'
    options['test_dir'] = 'tap/serial'
    call_task('test.pgtap')



@task
@cmdopts(PGTAP_OPTS)
@needs(['build.plugin'])
def pgtap_serial_all(options):
    """Integrated function to run all Pgtap tests which should be run serially(columnstore, configuration, replication, serial)"""
    call_task('test.pgtap_serial')
    call_task('test.pgtap_serial_columnstore')
    call_task('test.pgtap_configuration')
    # Need to run it before the replication tests because the latter uses 2 hardcoded configs for PRI/SBY
    call_task('test.pgtap_replication')

@task
@cmdopts([
    ('test=', 't', 'run specific test(s) by name or pattern (Catch2 test spec)'),
    ('list-tests', '', 'list all tests without running them'),
    ('list-tags', '', 'list all tags'),
    ('success', 's', 'show successful tests'),
    ('break', 'b', 'break into debugger on failure'),
    ('abort', 'a', 'abort on first failure'),
    ('abortx=', 'x', 'abort after x failures (default: 1)'),
    ('durations=', 'd', 'show test durations (yes, no; default: no)'),
])
@needs(['build.plugin', 'build.unit_tests'])
@mcp_enabled
def unit(options):
    """Run the Swarm64 DA PostgreSQL plugin tests that test specific components in unit level."""

    # Build Catch2 arguments from options
    catch2_args = []

    # Test pattern/name (positional)
    if options.get('test'):
        catch2_args.append(options['test'])

    # Keep original reporter logic (don't expose as option)
    reporter = '-r teamcity' if options.teamcity else ''
    if reporter:
        catch2_args.append(reporter)

    # Keep original color logic (don't expose as option)
    color = '' if options.teamcity else '--use-colour yes'
    if color:
        catch2_args.append(color)

    # Optional flags with sensible defaults
    if options.get('list_tests'):
        catch2_args.append('--list-tests')
    if options.get('list_tags'):
        catch2_args.append('--list-tags')
    if options.get('success'):
        catch2_args.append('-s')
    if options.get('break'):
        catch2_args.append('-b')
    if options.get('abort'):
        catch2_args.append('-a')

    # Abort after x failures (default: 1)
    if options.get('abortx'):
        catch2_args.append(f'-x {options["abortx"]}')

    # Durations (default: no)
    if options.get('durations'):
        catch2_args.append(f'-d {options["durations"]}')

    all_args = ' '.join(catch2_args)
    options['config'] = 'unit-tests'
    call_task('db.start_server')
    execute(options,
            ['psql -c "DROP DATABASE IF EXISTS unit_tests"',
              'psql -c "CREATE DATABASE unit_tests"',
              'psql unit_tests -c "CREATE EXTENSION swarm64da_unit_tests"',
             f'psql unit_tests -c "SELECT swarm64da_unit_tests.run_unit_tests(\'{all_args}\')"',
            ])



@task
@cmdopts([('test=', 't', 'run all tests within a class by providing the class_name or only a specific test by class_name.test_name'),
          ('debug', '', 'Run the tests in debug mode'),
          ('stacktrace', '', 'Print stack trace on failure'),
          ('cmd=', '' , 'Gradle command to run, default is "clean test"'),
          ('spotlessFiles=', '', 'Comma-separated list of files to check/apply with spotless')
         ])
@needs(['build.plugin_with_modlog_reader'])
@mcp_enabled
def junit(options):
    """Start the PostgreSQL server and launch the JDBC JUnit tests."""

    # Copy junit from the repo to the build directory so that we don't build in the repo directory.
    # Some people reported failures due to host_user not owning the mapped repo. This should have been fixed by now,
    # but building in the build directory rather than the repo directory is still a good practice.
    junit_repo_dir = f'{options.paths.dev}/db/tests/integration/junit'
    junit_build_dir = f'{options.paths.runner}/junit'
    execute(options, [
              f'rm -rf  {junit_build_dir}',
              f'cp -r {junit_repo_dir} {junit_build_dir}'
            ])

    test_files = options.junit.get('test', '*')
    if test_files == '*':
        test_files = '\'*\''

    debug = '--debug' if options.junit.get('debug') else ''
    stacktrace = '--stacktrace' if options.junit.get('stacktrace') else ''
    cmd = options.junit.get('cmd') if options.junit.get('cmd') else f'clean test --tests {test_files}'

    # We only need to start the database and run some SQL if command is 'test'.
    if ' test ' in cmd:
        options['config'] = 'primary-integration'
        call_task('db.start_primary')
        options['config'] = 'standby-integration'
        call_task('db.start_standby')
        options['config'] = 'read-replica-integration'
        call_task('db.start_read_replica')

        execute(options, [
                  'psql -c "DROP DATABASE IF EXISTS junit"',
                  'psql -c "CREATE DATABASE junit"',
                  'psql junit -c "CREATE EXTENSION swarm64da"'
                ])

    prop = f'SPOTLESS_FILES={options.junit.get("spotlessFiles")}' if options.junit.get('spotlessFiles') else ''
    execute(options, [
              f'{prop} gradle --no-daemon --project-dir {junit_build_dir} {debug} {stacktrace} {cmd}'
            ])


PSQL_REGRESS_OPTS = [
    ('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat'),
    ('skip_out_patch', '', 'Do not generate patch for expected dir'),
    ('preserve_test_dir', '', 'Preserve directory with the tests. Do not recreate it.'),
    ('test=', 't', 'run only a specific test by giving its path pattern.')
          ]

@task
@needs(['build.plugin'])
def psql_regress_all(options):
    """Start the PostgreSQL server and launch the native PostgreSQL regress tests with psql and mariadb semantic parity"""

    options['semantics'] = 'sql_compat'
    psql_regress(options)

    options['semantics'] = 'pg_native'
    psql_regress(options)

@task
@cmdopts(PSQL_REGRESS_OPTS)
@needs(['build.plugin'])
@mcp_enabled
def psql_regress(options):
    """Start the PostgreSQL server and launch the native PostgreSQL regress tests."""

    psql_test_run('regress', options)


@task
@consume_args
@needs(['build.plugin'])
@mcp_enabled
def psql_regress_crash_test(options, args):
    """Start the PostgreSQL server and launch the native PostgreSQL regress tests with all of Swarm64 DA enabled."""

    out_dir = f'{options.paths.artifacts}/psql_regress'
    options['config'] = 'crash-tests'
    call_task('db.start_server')
    execute(
            options,
            [f'mkdir -p {out_dir}',
              ':> /var/log/postgresql.log', # make sure we start with a clean state
	         f'cd {out_dir}',
              'rm -rf testtablespace',
              'mkdir -p testtablespace expected sql',
             f'($PG_BIN_DIR/pg_regress --load-extension=swarm64da {prod_extensions}'
              '--schedule=$PG_SOURCE/src/test/regress/parallel_schedule --max-connections=20 '
             f'--inputdir=$PG_SOURCE/src/test/regress --outputdir={out_dir}'
	          '; true)' # ignore the actual testresults, we just want to test crashes
            ]
    )
    # Need to stop server before grepping the log to make sure the log file is up to date.
    call_task('db.stop_server')
    execute(
            options,
            [
	          'grep \'the database system is in recovery mode\' /var/log/postgresql.log'
              ' && exit 1 || exit 0'
            ]
    )


def get_fix_permission_cmd(dir):
    return f'chown {dir} -Rv --reference {dir}/.. > /dev/null'

def psql_test_run(test_name, options):
    """ Prepare and run the given native PostgreSQL test with the given semantics. """

    semantics = options.get('semantics', 'sql_compat')
    if semantics == 'sql_compat':
        options['config'] = POSTGRES_SQL_COMPAT_CONF
    elif semantics == 'pg_native':
        options['config'] = POSTGRES_NATIVE_CONF
    else:
        LOG.error("semantics should be either 'pg_native' or 'sql_compat'")
        sys.exit(-1)

    LOG.info(f"Preparing for PostgreSQL {test_name} test with {semantics} semantics")

    # For sql_compat, we use our test location as test input. We backup our test folder,
    # then merge the original files with our changes. And then restore from backup after the test.
    if semantics == 'sql_compat':
        backup_dir = f'{options.paths.artifacts}/{test_name}_bak'
        input_dir = f'{options.paths.dev}/db/tests/integration/postgres/{test_name}'
        execute_root(
            options,
            [f'rm -rf {backup_dir} && mkdir {backup_dir}',
             f'cp -r {input_dir}/* {backup_dir}',
             f'cp -ru $PG_SOURCE/src/test/{test_name}/* {input_dir}',
             f'cp -r {backup_dir}/* {input_dir}',
             get_fix_permission_cmd(backup_dir),
             get_fix_permission_cmd(input_dir)
            ]
        )
    else:
        input_dir = f'$PG_SOURCE/src/test/{test_name}'

    out_dir = f'{options.paths.artifacts}/psql_{test_name}_{semantics}'

    test_files = []
    pattern = options.get('test', '*')

    if test_name == 'isolation':
        schedule = '--schedule=$PG_SOURCE/src/test/isolation/isolation_schedule'
        bin = 'pg_isolation_regress'
        file_pattern = f'{input_dir}/specs/{pattern}.spec'
    elif test_name == 'regress':
        schedule = '--schedule=$PG_SOURCE/src/test/regress/parallel_schedule'
        bin = 'pg_regress'
        file_pattern = f'{input_dir}/sql/{pattern}.sql'
    else:
        LOG.error("Unsupported test name")
        sys.exit(-1)
       

    if pattern != '*':
        schedule = ''
        test_files = list_matching_tests(file_pattern)
        if not test_files:
            print("No matching test found for pattern")
            return
        # For "regress", make sure test_setup is first.
        if test_name == 'regress':
            if 'test_setup' in test_files:
                test_files.remove('test_setup')
            test_files.insert(0, 'test_setup')

    call_task('db.start_server')

    test_cmd = (
        f'$PG_BIN_DIR/{bin} --load-extension=swarm64da {schedule} --max-connections=20 '
        f'--inputdir={input_dir} --outputdir={out_dir} {" ".join(test_files)}'
    )

    LOG.info(f"Running test command: {test_cmd}")

    try:
        execute(
            options,
            [f'mkdir -p {out_dir}',
             f'cd {out_dir}',
             'rm -rf testtablespace',
             'mkdir -p testtablespace expected sql',
             f'{test_cmd}',
            ]
        )
    finally:
        try:
            if pattern == '*':
                test_files = list_matching_tests(f'{input_dir}/sql/*.sql')
            pgregress_print_teamcity_status(options, out_dir, test_files)
        finally:
            LOG.info(f"Restoring original files from backup")
            if semantics == 'sql_compat':
                execute_root(
                    options,
                    [f'rm -rf {input_dir}/*',
                     f'cp -r {backup_dir}/* {input_dir}',
                     get_fix_permission_cmd(input_dir),
                    ]
                )


@task
@cmdopts([('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat'),
          ('test=', 't', 'run only a specific test by giving its path pattern.')])
@needs(['build.plugin'])
@mcp_enabled
def psql_isolation(options, ):
    """Start the PostgreSQL server and launch the native PostgreSQL isolation tests for specific configuration."""

    psql_test_run('isolation', options)


@task
@needs(['build.plugin'])
def psql_isolation_all(options):
    """Start the PostgreSQL server and launch the native postgreSQL isolation tests for all configurations."""

    options['semantics'] = 'sql_compat'
    psql_isolation(options)

    options['semantics'] = 'pg_native'
    psql_isolation(options)

@task
@consume_args
@needs(['build.plugin'])
@mcp_enabled
def psql_isolation_crash_test(options, args):
    """Start the PostgreSQL server and launch the native PostgreSQL isolation tests with all of Swarm64 DA enabled."""

    out_dir = f'{options.paths.artifacts}/psql_isolation'
    options['config'] = 'crash-isolation'
    call_task('db.start_server')
    execute(
            options,
            [f'mkdir -p {out_dir}',
              ':> /var/log/postgresql.log', # make sure we start with a clean state
	         f'cd {out_dir}',
              'rm -rf testtablespace',
              'mkdir -p testtablespace expected sql',
             f'($PG_BIN_DIR/pg_isolation_regress --load-extension=swarm64da {prod_extensions}'
              '--schedule=$PG_SOURCE/src/test/isolation/isolation_schedule --max-connections=20 '
             f'--inputdir=$PG_SOURCE/src/test/isolation --outputdir={out_dir}'
	          '; true)' # ignore the actual testresults, we just want to test crashes
            ]
    )
    # Need to stop server before grepping the log to make sure the log file is up to date.
    call_task('db.stop_server')
    execute(
            options,
            [
	          'grep \'the database system is in recovery mode\' /var/log/postgresql.log'
              ' && exit 1 || exit 0'
            ]
    )

@task
@cmdopts([('test=', 't', 'filter tests by substring. May match multiple tests. Do not include file extension.'),
          ('details', '', 'show detailed output about failed and executed tests')])
@needs(['build.plugin'])
@mcp_enabled
def system(options):
    """Start the PostgreSQL server and launch the system tests with all of Swarm64 DA enabled."""

    execute_root(options, ['chmod u+s $(which python3)'])
    pattern = options.get('test', '*')
    tests_dir = f'{options.paths.dev}/db/tests/system/'
    test_files = list_matching_tests(f'{tests_dir}/**/*{pattern}*.t', True)
    parallelism = options['building']['test-parallelism']

    if not test_files:
        print("No matching test found for pattern")
        return
    test_files_str=  ' '.join(test_files)
    if pattern != '*':
        tests_to_run = test_files_str
    else:
        tests_to_run = f'-r {tests_dir}'
    include_dir = f'$PG_SOURCE/src/test/perl/'
    pg_regress = f'PG_REGRESS=$PG_BIN_DIR/pg_regress'
    out_dir = f'{options.paths.tests}/system/'
    log_dir = f'{options.paths.artifacts}/log/system/'
    details_args = '--verbose ' if options.get('details') else ''

    output_format= '--nocolor' if options.teamcity else '--color'
    print('Running system tests with parallelism: ', parallelism)
    # Similar function to what's in pgtap to generate test command based on state.
    def gen_run_test_cmd(state):
        if state not in ['failed', 'save']:
            raise ValueError("Invalid value for 'state' parameter. Expected 'failed' or 'save'.")

        # Here, if state is 'failed' we run only failed tests but we point to the root directory, so that we can find
        # the failed tests. This takes assumption that initially we run in clean state, which is true on teamcity.
        _tests_arg = f'{tests_dir}' if state == 'failed' else f'{tests_to_run}'
        generate_state_file = f'--state {state}' if options.teamcity else ''
        cmd = (f'{pg_regress} prove {_tests_arg} {output_format} {details_args} '
              f'--formatter TAP::Formatter::File --failures --color --parse --normalize --timer '
              f'--merge -j {parallelism} {generate_state_file} -I {include_dir} -I {tests_dir}lib '
              f'''--rules='par=**parallel/*.t' || (egrep -rni "Segmentation fault|TRAP: FailedAssertion" {out_dir}/tmp_check/log && exit 1)''')
        return cmd

    # Load flaky tests if available.
    flaky_tests_file = os.path.join(tests_dir, 'flaky_tests')
    known_flaky_tests = []
    if os.path.exists(flaky_tests_file):
        with open(flaky_tests_file) as f:
            known_flaky_tests = f.read().splitlines()

    prove_path = os.path.join(options.paths.artifacts, '.prove')
    try:
        execute(options,
                [f'mkdir -p {out_dir}/tmp_check/',
                 f'mkdir -p {log_dir}',
                 f'rm -rf {out_dir}/tmp_check/log',
                 f'rm -rf {out_dir}/tmp_check/*data*',
                 f'cd {out_dir}',
                 # symlink the log dir so we get it in a more sensible location.
                 # unfortunately we cannot tell pg_prove where to put data
                 f'ln -s {log_dir} {out_dir}/tmp_check/log',
                 f'rm -rf {log_dir}/*'])

        # Run the tests.
        execute(options,
                [gen_run_test_cmd('save')],
                out_dir)
    except Exception as e:
        failed_tests = get_list_of_tap_failed_tests(prove_path, 'system', options=options)
        if options.teamcity:
            # We rerun even when the test is not marked as flaky to make identification of the new flaky tests easier.
            execute(options,
                    [f'rm -rf {out_dir}/tmp_check_old',
                    f'mv {out_dir}/tmp_check/ {out_dir}/tmp_check_old',
                    f'cd {out_dir}',
                    gen_run_test_cmd('failed')])
            if set(failed_tests) > set(known_flaky_tests):
                print('failed test is not in the list of known flaky tests')
                raise e
        else:
            raise e
    finally:
        if options.teamcity and not options.get('skip_test_report', False):
            execute(options, [f'cp -r {options.paths.tests}/system/.prove {prove_path}'], workdir='/')
            teamcity_report_test_status(prove_path, known_flaky_tests)
            if os.path.exists(prove_path):
                os.remove(prove_path)
            execute(options, [f'rm {options.paths.tests}/system/.prove'], workdir='/')


@task
@cmdopts([('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat'),
          ('packages=', '', 'choose a specific package. if not provided it does all')])
@needs(['build.plugin'])
@mcp_enabled
def psql_contrib(options, ):
    """Start the PostgreSQL server and launch native PostgreSQL contrib regression suite."""

    semantics = options.get('semantics', 'sql_compat')
    if semantics == 'sql_compat':
        options['config'] = POSTGRES_SQL_COMPAT_CONF
    elif semantics == 'pg_native':
        options['config'] = POSTGRES_NATIVE_CONF
    else:
        LOG.error("semantics should be either 'pg_native' or 'sql_compat'")
        sys.exit(-1)

    LOG.info(f"Running PostgreSQL contrib regression test with {semantics} semantics")

    contrib_packages = options.get('packages', 'adminpack amcheck '\
                        'auth_delay auto_explain bloom btree_gin '\
                        'btree_gist citext cube dblink dict_int '\
                        'dict_xsyn earthdistance file_fdw '\
                        'fuzzystrmatch hstore intagg intarray '\
                        'isn lo ltree oid2name old_snapshot pageinspect '\
                        'passwordcheck pg_buffercache pg_freespacemap '\
                        'pg_prewarm pg_stat_statements pg_trgm pg_surgery '\
                        'pgrowlocks pgstattuple pg_visibility postgres_fdw '\
                        'seg spi tablefunc tcn test_decoding tsm_system_rows '\
                        'tsm_system_time unaccent vacuumlo')
    subprocess.call(['git', 'restore', f'postgres/contrib'], cwd=f'{options.paths.dev}')

    execute(
        options,
        [f'patch -p1 -i {options.paths.dev}/db/tests/integration/psql/$PG_VERSION/{semantics}_semantics/contrib/contrib_combined_test.patch'],
        '$PG_SOURCE/contrib'
    )

    call_task('db.start_server')

    out_dir = f'{options.paths.artifacts}/psql_contrib'

    # It doesn't make sense to run tap tests without changes as they don't reuse existing server setup but create new server each time.
    # This is why we opt out from running them and only list the respective tests by using PROVE_FLAGS = --dry.
    execute(
            options,
            [f'cd $PG_BUILD_DIR/contrib',
             f'mkdir -p {options.paths.artifacts}/psql_contrib/sql',
             f'mkdir -p {options.paths.artifacts}/psql_contrib/expected',
             f'test -f $PG_PREFIX/lib/postgresql/regress.so || ln -sf $PG_TESTSUITE/regress/regress.so $PG_PREFIX/lib/postgresql/regress.so',
             f'''make installcheck REGRESS_OPTS+='--load-extension=swarm64da --outputdir={out_dir}' SUBDIRS='{contrib_packages}' PROVE_FLAGS="--dry" '''
            ]
    )

    subprocess.call(['git', 'restore', f'postgres/contrib'], cwd=f'{options.paths.dev}')


@task
@needs(['build.plugin'])
def psql_contrib_all(options):
    """Start the PostgreSQL server and launch the native PostgreSQL regress tests."""

    options['semantics'] = 'sql_compat'
    psql_contrib(options)

    options['semantics'] = 'pg_native'
    psql_contrib(options)

@task
@consume_args
@needs('build.plugin')
def columnar_stress(options, args):
    """Run columnar_stress_test.py to validate columnar tables under concurrent load & crashes."""
    setup_replication = options.get('setup_replication', True)

    if setup_replication:
        # Start primary, standby and read-replica with SOD configs reused here
        options['config'] = 'primary-sod'
        call_task('db.start_primary')
        options['config'] = 'standby-sod'
        call_task('db.start_standby')
        options['config'] = 'read-replica-sod'
        call_task('db.start_read_replica')
    else:
        # Fallback to single-node config
        options['config'] = options.get('config', 'script-of-doom')
        call_task('db.start_server')


    execute(
        options,
        [
            'pip3 install -r requirements.txt --user > /dev/null',
            ':> /var/log/postgresql-primary.log',
            ':> /var/log/postgresql-standby.log',
            ':> /var/log/postgresql-read-replica.log',
            'psql -c "DROP DATABASE IF EXISTS columnar_stress WITH (FORCE)"',
            'psql -c "CREATE DATABASE columnar_stress"',
            'python3 columnar_stress_test.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual'
    )

    # Shutdown cluster
    if setup_replication:
        call_task('db.stop_standby')
        call_task('db.stop_read_replica')
        call_task('db.stop_primary')
    else:
        call_task('db.stop_server')


@task
@consume_args
@needs('build.plugin')
def script_of_doom(options, args):
    """Run the script_of_doom to test data consistency during concurrent IUD, vaccuum and DDL operations."""
    setup_replication = options.get('setup_replication', True)
    if setup_replication:
        options['config'] = 'primary-sod'
        call_task('db.start_primary')
        options['config'] = 'standby-sod'
        call_task('db.start_standby')
    else:
        options['config'] = options.get('config', 'script-of-doom')
        call_task('db.start_server')

    # Do not pass runner args to script_of_doom.
    args_to_skip_sod = ['coverage.report', 'coverage.init', 'config']
    sod_args = [arg for arg in args if not any(skip in arg for skip in args_to_skip_sod)]

    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         ':> /var/log/postgresql.log', # make sure we start with a clean state
         'python3 script_of_doom.py ' + ' '.join(sod_args)
        ],
        f'{options.paths.dev}/db/tests/manual')
    if setup_replication:
        call_task('db.stop_standby')
        call_task('db.stop_primary')
    else:
        call_task('db.stop_server')
    if 'coverage.report' in args:
        call_task('coverage.report')

@task
@consume_args
@needs('build.plugin')
def chaos_inplace(options, args):
    """Run the chaos inplace update to test data consistency during concurrent updates + crashes."""

    options['config'] = 'primary-sod'
    call_task('db.start_primary')
    options['config'] = 'standby-sod'
    call_task('db.start_standby')
    options['config'] = 'read-replica-sod'
    call_task('db.start_read_replica')

    # Do not pass config arg to the script.
    args_to_skip = ['config']
    args = [arg for arg in args if not any(skip in arg for skip in args_to_skip)]

    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         ':> /var/log/postgresql-primary.log', # make sure we start with a clean state
         ':> /var/log/postgresql-read-replica.log',
         ':> /var/log/postgresql-standby.log',
         'python3 in_place_update_chaos_monkey.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual')

    call_task('db.stop_standby')
    call_task('db.stop_read_replica')
    call_task('db.stop_primary')



@task
@consume_args
@needs('build.plugin')
def ops_stress_loadgen(options, args):
    """Start server and run mem_load_gen script."""
    # Use script-of-doom for now.
    options['config'] = options.get('config', 'script-of-doom')
    call_task('db.start_server')

    args_to_skip_sod = ['coverage.report', 'coverage.init', 'config']
    sod_args = [arg for arg in args if not any(skip in arg for skip in args_to_skip_sod)]

    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         ':> /var/log/postgresql.log', # make sure we start with a clean state
         'python3 ops_stress_loadgen.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual')
    call_task('db.stop_server')

@task
@consume_args
@needs('build.plugin')
def planner_feedback_doom(options, args):
    """Run planner_feedback_doom script."""

    options['config'] = options.get('config', 'planner-feedback-doom')
    options['test_dir'] = 'tap/serial'

    call_task('db.start_server')
    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         'python3 planner_feedback_doom.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual')
    call_task('db.stop_server')

@task
@consume_args
@needs('build.plugin')
def start_server_with_mem_limit(options, args):
    """Start server with memoty_limit to later run script of doom"""
    update_limitations(options, '2g')
    options['config'] = 'script-of-doom-local'
    call_task('db.start_server')
    execute(options, ['psql'])
    update_limitations(options, -1)


@task
@consume_args
@needs('build.plugin')
def resource_leaks(options, args):
    """Run the resource_leaks to test resource leaks during concurrent IUD, vaccuum and DDL operations."""
    options['config'] = 'integration'
    call_task('db.start_server')
    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         'python3 resource_leaks.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual')

@task
@consume_args
@needs('build.plugin')
def obs_counters(options, args):
    """Run the obs_counters to test observability counters."""
    options['config'] = 'dev'
    call_task('db.start_server')
    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         'python3 obs_counters.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual')


@task
@consume_args
@needs('build.plugin')
def background_worker(options, args):
    """Run the background_worker to test the behavior of background columnstore update workers during concurrent IUD, vaccuum and DDL operations."""
    setup_replication = options.get('setup_replication', True)
    if setup_replication:
        options['config'] = 'primary-sod'
        call_task('db.start_primary')
        options['config'] = 'standby-sod'
        call_task('db.start_standby')
    else:
        options['config'] = options.get('config', 'script-of-doom')
        call_task('db.start_server')

    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         'python3 background_worker.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual')
    if setup_replication:
        call_task('db.stop_standby')
        call_task('db.stop_primary')
    else:
        call_task('db.stop_server')

@task
@consume_args
@needs('build.plugin')
def cache_size_limitation(options, args):
    """Run the cache_size_limitation to test the cache size limitation during concurrent IUD, vaccuum and DDL operations."""
    options['config'] = 'cache-size-limit-test'
    call_task('db.start_server')
    execute(
        options,
        ['pip3 install -r requirements.txt --user > /dev/null',
         'python3 cache_size_limitation.py ' + ' '.join(args)
        ],
        f'{options.paths.dev}/db/tests/manual')

@task
def product_lifecycle(options):
    """Tests the currently selected OS/host-database product lifecycle"""

    options['version'] = 'test'

    ctx = container.ContainerContext(options)
    container.create_product_files(ctx, options)
    container.build(ctx, options, 'product-lifecycle', 'product_lifecycle',
            {'BASE_IMAGE': container.base_image(ctx, options)})

@task
@consume_args
@needs('build.plugin')
def sqlsmith(options, args):
    """Run the sql_smith to test with randomly generated queries."""
    if (options['operating-system']) != OperatingSystem.ubuntu_2004.name:
        sys.exit("sqlsmith is available only in Ubuntu image. Use `./runner/runner operating-system=ubuntu_2004 sqlsmith` to run")
    options['config'] = 'sqlsmith'
    call_task('db.start_server')
    execute(
        options,
        ['python3 run_sqlsmith.py ' + ' '.join(args)],
        f'{options.paths.dev}/db/tests/manual')


@task
def test_file_backup_restore(options):
    """Tests the file backup/restore tool."""
    if options['operating-system'] != OperatingSystem.centos_7.name:
        sys.exit("Only supporting CentOS for this test (because that's what is used in the Lab)")
    print("Installing zstd...")
    execute_root(
        options,
        ['yum -y install epel-release > /dev/null',
         'yum -y install zstd-1.5.5-1.el7.x86_64 > /dev/null']
    )
    print("Running test...")
    execute(
        options,
        ['./test.sh'],
        f'{options.paths.dev}/tools/file-backup-restore/test'
    )


@task
@needs(['build.plugin'])
@mcp_enabled
def test_repmgr(options):
    """Performs smoke test of Repmgr extension."""
    if options['operating-system'] != OperatingSystem.rhel_8.name:
        sys.exit("Only supporting RHEL8 for this test as CentOS7 is EOL")
    if options['host-database'] != 'psql_15':
        sys.exit("Currently only PostgreSQL 15 is supported for this test")

    print("Downloading & installing repmgr...")
    execute_root(
        options,
        [
            'yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm',
            'yum -qy module disable postgresql',
            'yum install -y yum-utils',
            'yum install -y --downloadonly --downloaddir=/tmp/repmgr/ repmgr_15-5.5.0-1PGDG.rhel8',
            'useradd postgres',  # Add user as rpm installation requires it
            'rpm --force -i --nodeps /tmp/repmgr/repmgr_15*.rpm',
            # Copy extension files & fix dependencies
            'cp -ar /usr/pgsql-15/* $BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/',
            # Fixup directory structure
            'mv $BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/share/extension/* $BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/share/postgresql/extension/',
            'mv $BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/lib/repmgr.so $BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/lib/postgresql/',
        ]
    )

    print("Starting databases...")
    options['config'] = 'primary-repmgr'
    call_task('db.start_primary')
    options['config'] = 'standby-repmgr'
    call_task('db.start_standby')

    # Register the primary and standby nodes, implicitly installing the extension.
    print("Registering replicas")
    repmgr_bin = '$BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/bin'
    execute(options, [
        './repmgr --config-file /pg-data-primary/repmgr-primary.conf primary register --force',
        'sleep 3', # Allow primary to finish setup
        './repmgr --config-file /pg-data-standby/repmgr-standby.conf standby register --force --upstream-node-id=1',
    ], repmgr_bin,
    before_command='source /swarm/env.sh && export LD_LIBRARY_PATH=/usr/pgsql-15/lib:$BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/lib:$LD_LIBRARY_PATH')

    print("Validating node status")
    execute(options, [
        './repmgr --config-file /pg-data-primary/repmgr-primary.conf node status',
        './repmgr --config-file /pg-data-standby/repmgr-standby.conf node status',
    ], repmgr_bin,
    before_command='source /swarm/env.sh && export LD_LIBRARY_PATH=/usr/pgsql-15/lib:$BUILD_DIR/db/src/pg_build/psql_build$PG_PREFIX/lib:$LD_LIBRARY_PATH')

    # Execute basic smoke test.
    print("Running smoke test")
    execute(options, ['psql -f $DEV_DIR/db/tests/compatibility/repmgr.sql'])


@task
@needs('build.all')
def modlog_reader_test(options):
    """Tests the modlog reader tool."""

    print("Running modlog reader test...")
    execute(
        options,
        ['python3 test_modlog.py'],
        f'{options.paths.dev}/tools/modlog_reader/tests'
    )

@task
@consume_args
@needs(['build.build_modlog_reader_unit'])
def modlog_reader_unit(options, args):
    """Run the modlog_reader unit tests."""
    build_dir = f'{options.paths.build}/tools/modlog_reader/tests/unit'
    all_args = ' '.join(args)
    execute(options,
                [
                    f'{build_dir}/modlog_reader_test {all_args}'
                ])

def generate_test_files(options, template_dir, out_dir):
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir, exist_ok=True)
    env = Environment(loader=FileSystemLoader(template_dir))

    def generate_test_file(template_name, version_src, src_is_pro, version_dst, dst_is_pro):
        arguments = {
            'S64_VERSION_SRC': version_src,
            'S64_VERSION_DST': version_dst,
            'SRC_IS_PRO': 1 if src_is_pro else 0,
            'DST_IS_PRO': 1 if dst_is_pro else 0,
        }
        file_name = f'{out_dir}/{template_name}-{version_src}-{src_is_pro}-{version_dst}-{dst_is_pro}.t'
        test_content = env.get_template(f'{template_name}.tpl').render(arguments)
        with open(file_name, 'w') as file:
            file.write('# the file content is generated via template, do not update directly\n\n')
            file.write(test_content)

    # versions from snow/lib/Version.pm
    # exclude V238 as the related tests are present as real files
    # this list should reflect the versions that are used by real production instances
    # when updating the list, recheck if we can remove versions that are no longer used by real production instances
    versions = ['V260', 'V271', 'V280', 'V290', 'V291', 'V300', 'V301', 'V310', 'V320', 'V330', 'Vdev']
    for template_name in ['001-01-upgrade-happy-path', '001-02-upgrade-fallbacks']:
        for version_src in versions:
            for src_is_pro in [False, True]:
                for dev_is_pro in [False, True]:
                    generate_test_file(template_name, version_src, src_is_pro, 'Vdev', dev_is_pro)

@task
@cmdopts([('test=', 't', 'filter tests by substring. May match multiple tests. Do not include file extension.'),
          ('details', '', 'show detailed output about failed and executed tests'),
          ('skip-package', '', 'skip building a new rpm, to speed up when you have built one already'),
          ('include-all-upgrade-tests', '', 'include all upgrade tests')])
@needs(['tools.ensure_snc_provision_branch'])
def snow(options):
    if not is_snow_supported_os(options.operating_system):
       sys.exit(f'snow env is not supported in {options.operating_system.name}')

    # Local buildid is always 1.
    # Set the build number high enough for converge to take the current release config.
    if not options.teamcity:
        options['building']['package-buildid'] = 10001

    options['building']['postfix'] = "snowtesting"

    env = {
        'LOCAL_PSQL_PACKAGE_DIR': f'{options.paths.build}/packages',
    }

    generate_tests_folder = f'{options.paths.dev}/db/tests/snow/generate_tests'
    generate_test_files(options, f'{options.paths.dev}/db/tests/snow/templates', generate_tests_folder)

    pattern = options.get('test', '*')
    tests_dir = f'{options.paths.dev}/db/tests/snow/'
    test_files = list_matching_tests(f'{tests_dir}/**/*{pattern}*.t', full_path=True, recursive=True)

    # Option to run happy path upgrade tests or any other non upgrade tests if the arg is not specified,
    # to shorten the run time.
    include_all_upgrade_tests = options.get('include_all_upgrade_tests')
    if not include_all_upgrade_tests:
        test_files = sorted(filter(lambda test_file: 'happy-path' in test_file or '/001-' not in test_file, test_files))

    if not test_files:
        print("No matching test found for pattern")
        return
    if options.teamcity or not options.get('skip_package'):
        call_task('build.packages')

    test_files_str=  ' '.join(test_files)
    tests_to_run = test_files_str
    include_dir = f'$PG_SOURCE/src/test/perl/'
    # Always use the release version pg_regress binary as $PG_BIN_DIR doesn't change with different type of builds in snow env.
    pg_regress = f'PG_REGRESS=$PG_BIN_DIR/pg_regress'
    out_dir = f'{options.paths.artifacts}/snow_system'
    details_args = '--verbose ' if options.get('details') else ''

    output_format= '--nocolor' if options.teamcity else '--color'
    # Hardcode the parallelism to 8 for now to reduce inode clash between different dbi.
    # It can be restored to a larger value after all releases in test contain the fix for DEF0635138.
    parallelism = 8

    # Try to increase the log file count to keep by patching snc-provision.
    subprocess.run(["sed", "-i", "s/LOGFILE_SAVE_COUNT_DEFAULT = 50$/LOGFILE_SAVE_COUNT_DEFAULT = 5000/",
                    f'{options.paths.sncprovision}/snc/constants.rb'])
    execute_snow(options,
            [f'rm -rf {out_dir}',
             f'mkdir -p {out_dir}',
             f'cd {out_dir}',
             f'{pg_regress} prove {tests_to_run} {output_format} {details_args} '
             f"--formatter TAP::Formatter::File --failures --color --parse --normalize --timer -j {parallelism} --rules='par=**' "
             # add util functions from system tests as well
             f''' --merge -I {include_dir} -I {tests_dir}lib -I {tests_dir}testsuites -I '{options.paths.dev}/db/tests/system/lib' '''],
            environements=env)

@cmdopts([('glide-version=', '', 'glide version filename'),
          ('pro', '', 'test pro version'),
          ('skip-package', '', 'skip building a new rpm, to speed up when you have built one already')])
@needs(['tools.ensure_snc_provision_branch'])
def zboot(options):
    if not options.operating_system in [OperatingSystem.rhel_8]:
        sys.exit(f'zboot is not supported in {options.operating_system.name}')

    # Timing is critical, so always use release build.
    if options['building']['type'] != BuildingType.Release.name:
        sys.exit('zboot can only be run with release build')

    options['building']['postfix'] = 'zboottesting'

    # Local buildid is always 1.
    # Set the build number high enough for converge to take the current release config.
    if not options.teamcity:
        options['building']['package-buildid'] = 10001

    all_options.apply_changes(options)
    # Make sure clean only called once.
    options['building']['clean'] = BuildCleaning.nothing

    if options.teamcity or not options.get('skip_package'):
        call_task('build.packages')

    is_pro = options.get('pro', False)
    env = {
        'LOCAL_PSQL_PACKAGE_DIR': f'{options.paths.build}/packages',
    }

    db_port = 3400
    db_id = 'test'
    db_name = f'{db_id}_1'
    glide_user_pw = 'glide'
    glide_port = 16000
    glide_id = f'{db_id}001'
    glide_version = options.get('glide_version')
    if not glide_version:
        sys.exit('Please provide a glide version filename. You can find the available versions at https://artifact.devsnc.com/content/groups/stable/com/snc/glide-dist')

    glide_db_config = f'glide.db.rdbms=postgresql,glide.db.url=jdbc:postgresql://localhost:{db_port}/,glide.db.user={glide_user_pw},glide.db.password={glide_user_pw},glide.db.name={db_name}'
    zboot_extra_config = 'glide.ts.disable.optimize.table.index=true,glide.ts.indexer.thread.wait.time=1,glide.ts.indexer.silent.progress.update=true'
    log_artifact_dir = f"zboot-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    execute_root(options, [f'yum install -y rsync'])
    execute(options, [f'mkdir -p /var/log/{log_artifact_dir}'])
    execute_snow(options,
            [f'converge -s postgres id={db_id} port={db_port} size=pico version=$LOCAL_PSQL_PACKAGE_VERSION strict_config_version=false set_bin_nice_capability=true --force-value version',
             f'snow postgres-add-appuser {db_port} -- --role {glide_user_pw} --password {glide_user_pw} --schema {db_name}',
             f'snow reconverge {db_port} customer_database_name={db_name} database_features_list=raptordb.pro ' if is_pro else "echo 'Skip enabling pro'",
             f'converge -s glide port={glide_port} id={glide_id} glide_properties={glide_db_config},{zboot_extra_config} glide_version={glide_version}',
             # workaround slowness on generating certs for glide
             f'ln -sf /dev/urandom /dev/random',
             # disable glide log rotation to reduce complexity finding the final log line
             f"sed -i 's/FileHandler.rotatable = true/FileHandler.rotatable = false/' /glide/nodes/{glide_id}_{glide_port}/conf/logging.properties",
             # ignore errors caused by networking issue in the aws servers from common pool
             f'snow service {glide_port} start || /bin/true',
             f"echo 'waiting for zboot to finish...'",
             # wait for the final log line
             f"tail -f /glide/nodes/{glide_id}_{glide_port}/logs/localhost_log.txt | awk '/Completed: text index events process in/{{print;exit}}' ",
             f'snow service {glide_port} stop || /bin/true',
             f'mkdir -p /var/log/{log_artifact_dir}',
             # save logs to artifacts
             f'cp /glide/nodes/{glide_id}_{glide_port}/logs/localhost_log.txt /var/log/{log_artifact_dir}',
             f'cp /glide/postgres/{db_id}_{db_port}/log/postgresql-* /var/log/{log_artifact_dir}'],
            environements=env)
    options['zboot_log'] = f'{options.paths.log}/{log_artifact_dir}/localhost_log.txt'
    # save as html for reporting tab in teamcity
    options['out_file'] = f'{options.paths.runner}/artifacts/zboot.html'
    call_task('tools.parse_zboot_log')

@task
def glideIT_lifecycle(options):
    """Tests the service lifecycle for glide IT image"""
    if not is_snow_supported_os(options.operating_system):
        sys.exit(f'glideIT_lifecycle is not supported in {options.operating_system.name}')

    options['version'] = 'test'
    image = container.create_glideIT(options)

    try:
        client = docker.from_env()
    except Exception as e:
        LOG.error("Could not connect to docker daemon; make sure it is running")
        sys.exit(e)

    # Postgres should start without supplying a command.
    con = client.containers.run(image, detach=True)
    # Wait for the postgres service to start.
    time.sleep(5)
    exit_code, output = con.exec_run('psql -U dbi_3400 -d glide -h 127.0.0.1 -p 3400 -tA -c "select current_database()"')
    con.stop()
    if exit_code != 0 or output.decode().strip() != 'glide':
        LOG.error(f"Failed to verify glideIT docker image: {output.decode()}")
        sys.exit(1)

# Test that starts the server with specific configuration and verifies that the parameters are set correctly.
@task
@mcp_enabled
def self_hosted_configs(options):
    """Tests the self-hosted configurations."""

    # Exit if this is not release build.
    if options['building']['type'] != BuildingType.Release.name:
        sys.exit("self-hosted configs test can only be run with release build")

    call_task('build.plugin')

    config_files = ['local-test-self-hosted-small-size', 'local-test-self-hosted-peta-size', 'local-test-self-hosted-yotta-size']
    for config_file in config_files:
        try:
            # Remove 'local-test' from the config file name to match the CSV file name and append '.csv'.
            csv_file = config_file.replace('local-test-', '') + '.csv'
            LOG.info(f"Testing with configuration: {config_file} and CSV file: {csv_file}")
            options['config'] = config_file
            call_task('db.start_server')
            execute(
                options,
                ['pip3 install -r requirements.txt --user > /dev/null',
                ':> /var/log/postgresql.log', # make sure we start with a clean state
                f"python3 test_self_hosted_configs.py --csv-file self-hosted-expected/{csv_file}"
                ],
                f'{options.paths.dev}/db/tests/manual'
            )
            call_task('db.stop_server')

            # If this is not the last test, wait for the server to stop completely before starting the next one.
            if config_file != config_files[-1]:
                time.sleep(2)
        except Exception as e:
            LOG.error(f"Error while testing with configuration {config_file}: {e}")
            call_task('db.stop_server')
            sys.exit(1)

        LOG.info(f"Finished testing with configuration: {config_file}")

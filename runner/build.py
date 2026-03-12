import logging
import cmake
import all_options
from instance import *
from enums import *
from paver.easy import *
from mcp_runner.decorators import *
from git import Repo
import re
import credential
from all_options import parse_enum

LOG = logging.getLogger(__name__)

@task
def set_capabilities(options):
    """Sets/removes capabilities on the postgres binary relevant for the target build."""
    no_cap = options.get('disable_cap')
    cap_bin = options.get('cap_binary')

    # if valgrind or sanitizer is enabled, then dont set cap
    if (options.get('building')['type'] == BuildingType.Sanitizers.name or
	options.get('building')['use-valgrind']):
        no_cap = 'true'

    if no_cap != 'true':
      LOG.info(f'Setting capabilities: cap_sys_ptrace,cap_sys_nice=eip on {cap_bin}...')
      execute_root(options,
                 [
                     f'/usr/sbin/setcap cap_sys_ptrace,cap_sys_nice=eip {cap_bin};'
                     f'echo "Capability set on {cap_bin}: "$(/usr/sbin/getcap {cap_bin})"";'
                 ])
    else:
      LOG.info("Removing capabilities from postgres binary if they are present...")
      execute_root(options,
                 [
                     f'getcap {cap_bin} | grep -q "cap_" && /usr/sbin/setcap -r {cap_bin};'
                     f'echo "Capabilities removed from {cap_bin}...";'
                 ])

@task
@mcp_enabled
def plugin(options):
    """Build the database project."""
    cmake.build(options, targets = ['install_swarm64da_psql_plugin', 'install_swarm64da_confs'])

    options['cap_binary'] = '$PG_BIN_DIR/postgres'
    set_capabilities(options)

@task
@cmdopts([('what=', 'w', 'Choose what to clean: data,build,artifacts,build_cache,deterministic,nuclear; LLM_REQUIRE')])
@mcp_enabled
def clean(options):
    """Cleans up the container volumes."""
    options.build_cleaning = parse_enum(BuildCleaning, options.get('what', 'build'), 'what')
    cmake.cleanup(options)

@task
def plugin_with_modlog_reader(options):
    """Build (and install) the plugin with modlog reader."""
    cmake.build(options, targets = ['install_swarm64da_psql_plugin', 'install_swarm64da_confs', 'modlog_reader'])

@task
@cmdopts([('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat')])
def regress_patch(options):
    """Generates new patch files for the sql, input and expected files.
    First run 'runner psql_regress_prepare'. Then  make your changes to the sql and input files.
    Use 'runner psql_regress' to generate new result files, which can then be copied to /expected."""

    semantics = options.get('semantics', 'sql_compat')
    if semantics != 'pg_native' and semantics != 'sql_compat':
        LOG.error("semantics should be either 'sql_compat' or 'pg_native'")
        sys.exit(-1)

    trg_path = options.paths.dev + f'/db/tests/integration/psql/$PG_VERSION/{semantics}_semantics/pg_regress'
    src_path = f'{options.paths.artifacts}/psql_regress/{semantics}_semantics'

    # folders we want to patch are different in pg14 and pg15
    patch_folders = ['sql', 'expected', 'data']

    for folder_name in patch_folders:
        execute_root(options,
                 [
                     f'''diff -r -U4 $PG_SOURCE/src/test/regress/{folder_name} {folder_name} | grep -v '^Only'> {trg_path}/{folder_name}.patch;'''
                     f'echo "generated {trg_path}/{folder_name}.patch";'
                     f'''sed -i "s|$PG_SOURCE|/usr/local/postgresql-src|g" {trg_path}/{folder_name}.patch;'''
                     f'echo "Standardized paths in {trg_path}/{folder_name}.patch";'
                     ],
                 f'{src_path}')

@task
@cmdopts([('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat')])
def contrib_regress_patch(options):
    """Generates new combined patch file for contrib tests.  
    First run 'runner psql_contrib' to generate new result files. 
    Then run contrib_create_patch_dirs. 
    Copy or update result/sql files in the directory related to the necessary test suite. 
    Parent directory to be updated is pointed by contrib_create_patch_dirs """

    # We don't distinguish between sql and expected files here 
    # for simplicity as the folder structure is complex and not always the same between tests suites in the contrib module.

    semantics = options.get('semantics', 'sql_compat')
    if semantics != 'pg_native' and semantics != 'sql_compat':
        LOG.error("semantics should be either 'sql_compat' or 'pg_native'")
        sys.exit(-1)

    trg_path = options.paths.dev + f'/db/tests/integration/psql/$PG_VERSION/{semantics}_semantics/contrib'
    execute_root(options,
                 [
                     f'cd {options.paths.artifacts}',
                     f'''diff -r -U3 contrib_orig contrib_updated | grep -v '^Only'> {trg_path}/contrib_combined_test.patch;'''
                     f'echo generated {trg_path}/contrib_combined_test.patch'],
                 '${BUILD_DIR}')

@task
@cmdopts([('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat')])
def contrib_create_patch_dirs(options):
    """Generates contrib_updated directory in the artifacts which has to be updated with new sql/expected results which differ from the original."""

    semantics = options.get('semantics', 'sql_compat')
    if semantics != 'pg_native' and semantics != 'sql_compat':
        LOG.error("semantics should be either 'sql_compat' or 'pg_native'")
        sys.exit(-1)
    execute(options,
                 [
                     f'cp -rn $PG_SOURCE/contrib {options.paths.artifacts}/contrib_orig',
                     f'cd {options.paths.artifacts}',
                     f'cp -r contrib_orig contrib_updated',
                     f'cd contrib_updated',
                     # apply already existing patch so that less files need to be changed
                     f'patch -p1 -i {options.paths.dev}/db/tests/integration/psql/$PG_VERSION/{semantics}_semantics/contrib/contrib_combined_test.patch',
                     f'echo created test directories. Change files in {options.paths.artifacts}/contrib_updated.'],
                 '${BUILD_DIR}')

@task
@cmdopts([('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat')])
def isolation_regress_patch(options):
    """Generates new combined patch file for isolation tests.
    First run 'runner psql_isolation' to generate new result files.
    Then run isolation_create_patch_dirs.
    Copy or update result/sql files in the directory related to the necessary test suite.
    Parent directory to be updated is pointed by isolation_create_patch_dirs """

    # We create the patch for the expected files here 

    semantics = options.get('semantics', 'sql_compat')
    if semantics != 'pg_native' and semantics != 'sql_compat':
        LOG.error("semantics should be either 'sql_compat' or 'pg_native'")
        sys.exit(-1)

    trg_path = options.paths.dev + f'/db/tests/integration/psql/$PG_VERSION/{semantics}_semantics/isolation'
    execute_root(options,
                 [
                     f'cd {options.paths.artifacts}',
                     f'''diff -r -U3 isolation_orig isolation_updated | grep -v '^Only'> {trg_path}/isolation_expected_test.patch;'''
                     f'echo generated {trg_path}/isolation_expected_test.patch'],
                 '${BUILD_DIR}')

@task
@cmdopts([('semantics=', '', 'Choose semantics, must be either pg_native or sql_compat')])
def isolation_create_patch_dirs(options):
    """Generates isolation_updated directory in the artifacts which has to be updated with new expected results which differ from the original."""

    semantics = options.get('semantics', 'sql_compat')
    if semantics != 'pg_native' and semantics != 'sql_compat':
        LOG.error("semantics should be either 'sql_compat' or 'pg_native'")
        sys.exit(-1)
    execute(options,
                 [
                     f'rm -rf {options.paths.artifacts}/isolation_orig',
                     f'cp -rn $PG_SOURCE/src/test/isolation/expected/ {options.paths.artifacts}/isolation_orig',
                     f'cd {options.paths.artifacts}',
                     f'rm -rf isolation_updated',
                     f'cp -r isolation_orig isolation_updated',
                     f'cd isolation_updated',
                     # apply already existing patch so that less files need to be changed
                     f'patch -p1 -i {options.paths.dev}/db/tests/integration/psql/$PG_VERSION/{semantics}_semantics/isolation/isolation_expected_test.patch',
                     f'echo created test directories. Change files in {options.paths.artifacts}/isolation_updated.'],
                 '${BUILD_DIR}')

@task
@mcp_enabled
def unit_tests(options):
    """Build (and install) the the unit tests."""
    cmake.build(options, targets = ['install_swarm64da_db_unit_tests_plugin', 'install_swarm64da_confs'])

NEXUSOPTS = [('push_to_nexus=', '', 'Should push rpm to nexus')]

@task
@mcp_enabled
@cmdopts(NEXUSOPTS)
def packages(options):
    """Build packages and place them in the artifacts directory. Also upload the rpm to Nexus if requested."""

    branch = Repo(options.paths.dev).active_branch.name
    if not branch.startswith("release/") and not options['building']['postfix']:
        LOG.warning("only release branches are allowed without postfix; adding a default postfix")
        options['building']['postfix'] = "not-for-distribution"

    if options['building']['package-kind'] == 'modlog_reader' and options.teamcity:
        options['building']['postfix'] = ""

    # first cleanup the package dir because we really don't want old packages there
    execute_root(options, [f'rm -rf {options.paths.build}/packages {options.paths.artifacts}/packages'])
    cmake.build(options, targets = ['all'])
    # we run cmake package as root because otherwise the user-ids are screwed-up
    execute_root(options, [f'ninja package'], options.paths.build)
    execute(options, [f'mkdir -p {options.paths.artifacts}/ && cp -a {options.paths.build}/packages {options.paths.artifacts}/'])

    if not instance_is_mac():
        # fixup all permission problems that were created by running package creation as root
        execute_root(options, [f'chown host_user:host_group -R {options.paths.build}'])

        push_to_nexus = options.get('push_to_nexus')
        nexus_user = credential.get_credential(options, 'NEXUS_USER')
        nexus_password =  credential.get_credential(options, 'NEXUS_PASSWD')
        if options.teamcity and push_to_nexus == 'true' and nexus_user is not None and nexus_password is not None:
            execute_root(options, [f'find {options.paths.artifacts} -name \'*.rpm\' ' \
            f'| xargs -I{{}} {options.paths.dev}/tools/deploy_rpm_to_nexus.sh {nexus_user}:{nexus_password} {{}}'])

@task
def all(options):
    """Build everything"""
    cmake.build(options, targets = ['all'])

@task
def build_modlog_reader_unit(options):
    """Build modlogreader unit tests"""
    cmake.build(options, targets = ['modlog_reader_test'])

@task
@mcp_enabled
def clang_tidy(options):
    """Run clang-tidy on everything"""
    options['building']['clang-tidy'] = True
    all_options.apply_changes(options)
    call_task('build.all')

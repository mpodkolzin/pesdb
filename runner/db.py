import logging
import atexit
from instance import *
from cmake import setup
from paver.easy import *
from enums import *
from mcp_runner.decorators import *
from optparse import OptionParser, make_option

LOG = logging.getLogger(__name__)

@task
@no_help
@needs(['build.plugin'])
def start_server(options):
    # make very sure its stopped so we always get the server with the right config
    stop_server(options)
    # Also stop replication primary to avoid conflict as they use the same port
    stop_primary(options)
    if options.building['use-valgrind'] == True:
        if options['operating-system'] != OperatingSystem.rhel_8.name:
            LOG.error("valgrind is supported only with rhel_8. The option is ignored.")
        if options['building']['type'] != BuildingType.Debug.name:
            LOG.error("valgrind is supported only with with debug build type. The option is ignored.")

    config = options.get('config', 'dev')

    execute(options, [f'/swarm/pg_start_dev.sh {config}'])

    atexit.register(stop_server)

@task
@no_help
@needs(['build.plugin'])
def start_primary(options):
    # make very sure its stopped so we always get the server with the right config
    stop_primary(options)
    # Also stop non-replication server to avoid conflict as they use the same port
    stop_server(options)
    config = options.get('config', 'primary')
    execute(options, [f'PG_DATA=$PG_DATA_PRIMARY /swarm/pg_start_dev.sh {config}'])

    atexit.register(stop_primary)

@task
@no_help
@needs(['build.plugin'])
def start_standby(options):
    # make very sure its stopped so we always get the server with the right config
    stop_standby(options)
    config = options.get('config', 'standby')
    execute(options, [f'PGPORT=5433 PG_DATA=$PG_DATA_STANDBY /swarm/pg_start_dev.sh {config}'])

    atexit.register(stop_standby)

@task
@no_help
@needs(['build.plugin'])
def start_read_replica(options):
    # make very sure its stopped so we always get the server with the right config
    stop_read_replica(options)
    config = options.get('config', 'read_replica')
    execute(options, [f'PGPORT=5434 PG_DATA=$PG_DATA_READ_REPLICA /swarm/pg_start_dev.sh {config}'])

    atexit.register(stop_read_replica)

@task
@no_help
def stop_server(options):
    execute(options, ['/swarm/pg_stop.sh'])

@task
@no_help
def stop_primary(options):
    execute(options, ['PG_DATA=$PG_DATA_PRIMARY /swarm/pg_stop.sh'])

@task
@no_help
def stop_standby(options):
    execute(options, ['PG_DATA=$PG_DATA_STANDBY /swarm/pg_stop.sh'])

@task
@no_help
def stop_read_replica(options):
    execute(options, ['PG_DATA=$PG_DATA_READ_REPLICA /swarm/pg_stop.sh'])

PGDB_OPTS = [
    make_option('-c', '--config', default='dev', type='string', help='db configuration to use (e.g. dev)')
]

@task
@cmdopts(PGDB_OPTS)
@needs(['start_server'])
@mcp_enabled(interactive = True, prompt_pattern=r"PG#{3}")
def client(options):
    """Interact via a PSQL client with the PG server."""

    if not options.mcp_runner:
        execute(options, ['psql'])
    else:
        # - print a "build complete" message so that the MCP server knows when the task has truly started.
        # - '-X' so that we don't read custom .psqlrc that the AI cannot deal with.
        # - don't stop on error, as we want to be able to continue to do things after and not restart every time.
        # - set simple PROMPT1/PROMPT2 so prompt pattern matching works reliably
        execute(options, ['script -q /dev/null -c "/swarm/ai_psql.sh"'])

@task
@cmdopts([('machine', '', 'use mi2 mode(machine interpretable mode); LLM_REQUIRE')])
@mcp_enabled(interactive = True, async_requires="db.client", prompt_pattern=r"\(gdb\)")
def gdb_client(options):
    """Interact via GDB to the backend process of the client USING MI2 MODE (do NOT try to attach with a specific PID, it will attach automatically)"""

    execute(options, ['psql -Atqc "select pid from pg_stat_activity where application_name=\'runner_client\' order by state_change desc limit 1" > /tmp/psql-client.pid'])
    if options.get('machine', False):
        execute_root(options, ['gdb --interpreter=mi2 -ex "set pagination off" -ex "set confirm off" -p $(cat /tmp/psql-client.pid)'])
    else:
        execute_root(options, ['gdb -p $(cat /tmp/psql-client.pid)'])

@task
@mcp_enabled(interactive = True, async_requires="db.client")
def tail_db_log(options):
    """Tail all DB logs to see e.g. stacktraces, etc."""
    
    execute(options, ['tail -f /var/log/postgresql*.log'])

@task
@cmdopts([('line-count=', '', 'number of lines to show')])
@mcp_enabled()
def last_log_lines(options):
    """Show the last N lines of the DB logs."""

    lines = options.get('line-count', '100')
    execute(options, [f'tail -n {lines} /var/log/postgresql*.log'])

@cmdopts([('config=', '', 'db configuration to use (e.g. default, integration)')])
@task
def replication_servers(options):
    """Start a db primary, standby and read-replica server then provide a terminal."""
    config = options.get('config', '')

    config_primary = f'primary-{config}' if config else 'primary'
    config_standby = f'standby-{config}' if config else 'standby'
    config_read_replica = f'read-replica-{config}' if config else 'read-replica'
    options['config'] = config_primary
    call_task('db.start_primary')
    options['config'] = config_standby
    call_task('db.start_standby')
    options['config'] = config_read_replica
    call_task('db.start_read_replica')

    print("Primary is available on :5432, standby on :5433, read-replica on :5434.")
    print("Easiest is to expose all ports to the outside using docker.ports")
    print("PRI: psql -p 5432")
    print("SBY: psql -p 5433")
    print("RR: psql -p 5434")
    execute(options, ['/bin/bash'])

@task
@cmdopts(PGDB_OPTS)
@needs(['build.plugin', 'start_server'])
def server_shell(options):
    """Start a shell after the db server has started"""
    execute(options, ['/bin/bash'], options.paths.build)


@task
@cmdopts(PGDB_OPTS)
@needs(['build.plugin', 'start_server'])
def server_shell_root(options):
    """Start a shell after the db server has started"""
    execute_root(options, ['/bin/bash'], options.paths.build)

@task
@cmdopts(PGDB_OPTS)
def vim(options):
   """Start vim in a root shell"""
   execute_root(options, ['reset', '/usr/local/bin/vim'], options.paths.dev)
   execute_root(options, ['/bin/bash'], options.paths.dev)
   atexit.register(stop_server)

@task
@cmdopts(PGDB_OPTS)
def nvim(options):
   """Start nvim in a root shell"""
   execute_root(options, ['/opt/nvim-linux64/bin/nvim --headless "+Lazy! install" +qa'], options.paths.dev)
   execute_root(options, [
        'mkdir -p ~/.local/share/nvim/lazy/vimspector/gadgets/linux &&'
        'cp $DEV_DIR/tools/nvim/vimspector.gadgets.json ~/.local/share/nvim/lazy/vimspector/gadgets/linux/.gadgets.json'])
   execute_root(options, ['reset', '/opt/nvim-linux64/bin/nvim'], options.paths.dev)
   execute_root(options, ['/bin/bash'], options.paths.dev)
   atexit.register(stop_server)


@task
@needs(['tools.ensure_snc_provision_branch'])
def snow(options):
    """Start snow environment in a root shell"""
    execute_snow(options)

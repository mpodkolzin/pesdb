from paver.easy import *
import all_options

@task
@no_help
def auto():
    all_options.load(options)
    all_options.apply_changes(options)

# Import all modules holding other tasks
import build
import container
import coverage
import db
import instance
import make_config
import targets
import test
import tools

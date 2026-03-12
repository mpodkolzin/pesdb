import logging
import os
import re
import tempfile
import subprocess
import sys

from jinja2 import Environment, FileSystemLoader
from ruamel import yaml

LOG = logging.getLogger(__name__)

def setup(options, env_vars):
    """Setup all necessary brew packages."""
    docker_dir = f'{options.paths.dev}/docker'
    operating_system = options.operating_system.name.lower()
    host_database = options.host_database.name.lower()
    file = f'{operating_system}/{host_database}'

    install_paths = options['mac']['install_paths'] + [options.paths.mac_native]

    # add the container.py arguments
    arguments = {**env_vars, 
        'HOST_DATABASE': host_database.upper(),
        'OPERATING_SYSTEM': operating_system.upper(),
        'ARCH': options.architecture.name.lower(),
        'INSTALL_PREFIX': options.paths.mac_native,
        'GLIDE_PREFIX': options.paths.mac_glide,
        'FORCE_REINSTALL': 1 if options['mac']['force-reinstall'] else 0,
        'SYS_ROOT': options['mac']['sys_root'],
        'PKG_CONFIG_PATHS': ":".join(options['mac']['pkg_config_paths']),
        'INCLUDE_PATHS': ":".join(f"{item}/include" for item in install_paths),
        'LIBRARY_PATHS': ":".join(f"{item}/lib" for item in install_paths),
        'LINK_FLAGS': " ".join(f"-L{item}/lib" for item in install_paths)
    }

    tmp = tempfile.NamedTemporaryFile()
    jinja_env = Environment(loader=FileSystemLoader(docker_dir))
    template = jinja_env.get_template(file).render(arguments)
    # remove comments as that eats newlines
    template = "#!/bin/bash" + re.sub(r'#.*\n', '', template, flags=re.MULTILINE)
    # change RUN <script> into an EOF based delimiter so that bash doesn't interpret it
    fixed_script = re.sub(
        r"RUN\s((?:.*\\\n|.)+.*)",  # Match RUN and all lines following ending on \
        lambda m: f"RUN <<'EOF'\n{m.group(1).strip()}\nEOF",
        template)
    tmp.write(fixed_script.encode())
    tmp.flush()

    os.chmod(tmp.name, 0o700)
    subprocess.check_call(tmp.name)

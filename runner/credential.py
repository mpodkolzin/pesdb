import os

def get_credential_from_config(options, key):
    return options['credential'][key]

def get_credential_from_env(key):
    return os.environ.get(key)

# yaml config take precedence than env variables
def get_credential(options, key):
    return get_credential_from_config(options, key) or get_credential_from_env(key)
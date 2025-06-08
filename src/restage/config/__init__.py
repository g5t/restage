import confuse
from os import environ
# Any platform independent configuration settings can go in 'default.yaml'
config = confuse.LazyConfig('restage', __name__)

# use environment variables specified as 'RESTAGE_XYZ' as configuration entries 'xyz'
config.set_env()
# Expected environment variables:
#   RESTAGE_FIXED="/loc/one /usr/loc/two"
#   RESTAGE_CACHE="$HOME/loc/three"


def _common_defaults():
    import yaml
    from importlib.resources import files, as_file

    common_file = files(__name__).joinpath('default.yaml')
    if not common_file.is_file():
        raise RuntimeError(f"Can not locate default.yaml in module files (looking for {common_file})")
    with as_file(common_file) as file:
        with open(file, 'r') as data:
            common_configs = yaml.safe_load(data)

    return common_configs or {}


# By using the 'add' method, we set these as the *lowest* priority. Any user/system files will override:
config.add(_common_defaults())
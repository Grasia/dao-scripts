from typing import Any

import re

from pathlib import Path
from argparse import Namespace

from dynaconf import Dynaconf, Validator

_units = {
    "B": 1, 
    "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12,
    "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40,
}

def parse_size(size):
    if isinstance(size, int):
        return size

    size = size.upper()
    if not re.match(r' ', size):
        size = re.sub(r'([KMGT]?B)', r' \1', size)
    number, unit = [string.strip() for string in size.split()]
    return int(float(number)*_units[unit])

# TODO: Add some way of making very Runner capable of definig its config
# there somehow
_RUNNER_VALIDATORS: list[Validator] = [
    Validator('daohaus.skip_names', cast=bool, default=False),
    
    Validator('daostack.registered_only', cast=bool, default=True),
]

settings = Dynaconf(
    envvar_prefix="DAOA",
    validate_on_update=True,
    validators=[
        Validator('SKIP_INVALID_BLOCKS', cast=int, default=250),
        Validator('DEFAULT_DATAWAREHOUSE', cast=Path, default=Path("datawarehouse")),
        Validator('LOGGING_BACKUP_COUNT', cast=int, default=3),
        Validator('LOGGING_MAX_SIZE', cast=parse_size, default="100MB"),
        Validator('CC_API_KEY', default=""),
        Validator('THE_GRAPH_API_KEY', default=""),

        # Can be overriden by argparser
        Validator('run_only_updatable', cast=bool, default=False),
        Validator('DEBUG', cast=bool, default=False),
        Validator('raise_runner_errors', cast=bool, default=False),
        Validator('skip_token_balances', cast=bool, default=False),

        *_RUNNER_VALIDATORS,
    ]
)

def _sanitize_argname(name: str) -> str:
    return name.replace(".", "__")

def args2config(args: Namespace):
    argsdict: dict[str, Any] = vars(args)

    all_names = [ (vn,_sanitize_argname(vn)) for v in settings.validators for vn in v.names ]
    settings_update = { vn:(argsdict[an] or settings[vn]) for vn, an in all_names if an in argsdict }

    settings.update(settings_update)

def __getattr__(name):
    """
    Called when no function has been defined. Defaults to search argsparser.
    """
    return settings[name]

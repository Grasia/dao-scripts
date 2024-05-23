#!/usr/bin/env python3
from datetime import datetime
from pathlib import Path
import portalocker as pl
import os
import tempfile
import shutil
import sys
from sys import stderr

import logging
from logging.handlers import RotatingFileHandler

from argparse import Namespace

from .aragon.runner import AragonRunner
from .daohaus.runner import DaohausRunner
from .daostack.runner import DaostackRunner
from .common import ENDPOINTS
from .common.graphql import NetworkRunner
from .argparser import CacheScriptsArgParser
from ._version import __version__
from . import config

LOG_FILE_FORMAT = "[%(levelname)s] - %(asctime)s - %(name)s - : %(message)s in %(pathname)s:%(lineno)d"
LOG_STREAM_FORMAT = "%(levelname)s: %(message)s"

AVAILABLE_PLATFORMS: dict[str, type[NetworkRunner]] = {
    AragonRunner.name: AragonRunner,
    DaohausRunner.name: DaohausRunner,
    DaostackRunner.name: DaostackRunner
}

# Get available networks from Runners
AVAILABLE_NETWORKS = {n for n in ENDPOINTS.keys() if not n.startswith('_')}

def _call_platform(platform: str, datawarehouse: Path, force: bool=False, networks=None, collectors=None, block_datetime=None):
    p = AVAILABLE_PLATFORMS[platform](datawarehouse)
    p.run(networks=networks, force=force, collectors=collectors, until_date=block_datetime)

def _is_good_version(datawarehouse: Path) -> bool:
    versionfile = datawarehouse / 'version.txt'
    if not versionfile.is_file():
        return False

    with open(versionfile, 'r') as vf:
        return vf.readline().strip() == __version__

def main_aux(
    datawarehouse: Path, delete_force: bool, 
    platforms: list[str], networks: list[str], collectors: list[str], 
    block_datetime: datetime, force: bool, debug: bool = False,
):
    if delete_force or not _is_good_version(datawarehouse):
        if not delete_force:
            print(f"datawarehouse version is not version {__version__}, upgrading")

        # We skip the dotfiles like .lock
        for p in datawarehouse.glob('[!.]*'):
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()

    logger = logging.getLogger('dao_analyzer')
    logger.propagate = True
    filehandler = RotatingFileHandler(
        filename=datawarehouse / 'cache_scripts.log',
        maxBytes=int(config.LOGGING_MAX_SIZE),
        backupCount=int(config.LOGGING_BACKUP_COUNT),
    )

    filehandler.setFormatter(logging.Formatter(LOG_FILE_FORMAT))
    logger.addHandler(filehandler)
    logging.getLogger('gql.transport.requests').addHandler(filehandler)

    if config.DEBUG:
        logger.setLevel(level=logging.DEBUG)
        logging.getLogger('gql.transport.requests').setLevel(level=logging.DEBUG)

    # Log errors to STDERR
    streamhandler = logging.StreamHandler(stderr)
    streamhandler.setLevel(logging.WARNING if debug else logging.ERROR)
    streamhandler.setFormatter(logging.Formatter(LOG_STREAM_FORMAT))
    logger.addHandler(streamhandler)
    logging.getLogger('gql.transport.requests').addHandler(streamhandler)

    logger.info("Running dao-scripts with arguments: %s", sys.orig_argv)

    # The default config is every platform
    if not platforms:
        platforms = list(AVAILABLE_PLATFORMS.keys())

    # Now calling the platform and deleting if needed
    for platform in platforms:
        _call_platform(platform, datawarehouse, force, networks, collectors, block_datetime)

    # write date
    data_date: str = str(datetime.now().isoformat())

    if block_datetime:
        data_date = block_datetime.isoformat()

    with open(datawarehouse / 'update_date.txt', 'w') as f:
        print(data_date, file=f)

    with open(datawarehouse / 'version.txt', 'w') as f:
        print(__version__, file=f)

def main_lock(args: Namespace):
    datawarehouse = args.datawarehouse
    datawarehouse.mkdir(exist_ok=True)
    
    # Lock for the datawarehouse (also used by the dash)
    p_lock: Path = datawarehouse / '.lock'

    # Exclusive lock for the chache-scripts (no two cache-scripts running)
    cs_lock: Path = datawarehouse / '.cs.lock'

    try:
        with pl.Lock(cs_lock, 'w', timeout=1) as lock, \
             tempfile.TemporaryDirectory(prefix="datawarehouse_") as tmp_dw_str:

            # Writing pid and dir name to lock (debugging)
            tmp_dw = Path(tmp_dw_str)
            print(os.getpid(), file=lock)
            print(tmp_dw, file=lock)
            lock.flush()

            ignore = shutil.ignore_patterns('*.log', '.lock*')

            # We want to copy the dw, so we open it as readers
            p_lock.touch(exist_ok=True)
            with pl.Lock(p_lock, 'r', timeout=1, flags=pl.LOCK_SH | pl.LOCK_NB):
                shutil.copytree(datawarehouse, tmp_dw, dirs_exist_ok=True, ignore=ignore)

            main_aux(
                datawarehouse=tmp_dw,
                delete_force=args.delete_force,
                platforms=args.platforms,
                networks=args.networks,
                collectors=args.collectors,
                block_datetime=args.block_datetime,
                force=args.force,
            )

            with pl.Lock(p_lock, 'w', timeout=10):
                shutil.copytree(tmp_dw, datawarehouse, dirs_exist_ok=True, ignore=ignore)

            # Removing pid from lock
            lock.truncate(0)
    except pl.LockException:
        with open(cs_lock, 'r') as f:
            pid = int(f.readline())

        print(f"The cache_scripts are already being run with pid {pid}", file=stderr)
        exit(1)

def main():
    parser = CacheScriptsArgParser(
        available_platforms=list(AVAILABLE_PLATFORMS.keys()),
        available_networks=AVAILABLE_NETWORKS)

    args = parser.parse_args()
    config.args2config(args)

    if args.display_version:
        print(__version__)
        exit(0)

    main_lock(args)

if __name__ == '__main__':
    main()
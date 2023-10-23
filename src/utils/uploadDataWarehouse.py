#!/usr/bin/env python3
import os
import sys
import tempfile
from pathlib import Path
import shutil
import json
import requests
from time import sleep

from tqdm import tqdm

DEFAULT_DATAWAREHOUSE = Path(os.getenv('DAOA_DW_PATH', 'datawarehouse'))

def getDwPaths():
    """ Returns dw paths """
    return [
        './datawarehouse/'
    ]

def archivedw(dw, tmpdir):
    import pandas as pd

    paths = []
    
    for f in dw.glob('*.txt'):
        shutil.copy(f, tmpdir)
        paths.append(f)

    for f in dw.glob('*/metadata.json'):
        newp = tmpdir / Path(f).relative_to(dw).parent
        newf = newp / Path(f).name

        newp.mkdir(exist_ok=True)
        shutil.copy(f, newf)
        paths.append(newf)
    
    for f in tqdm(list(dw.glob('**/*.arr'))):
        newp = tmpdir / Path(f).relative_to(dw).parent
        newf = newp / Path(f).with_suffix('.csv').name

        pd.read_feather(f).to_csv(newf)
        paths.append(newf)

    return paths

def uploadToZenodo(paths):
    ZENODO_DEPOSITION_ID = os.environ['ZENODO_DEPOSITION_ID']
    ZENODO_SANDBOX = bool(os.environ.get('ZENODO_SANDBOX', False))

    from zenodo_client import Zenodo

    z = Zenodo(None, sandbox=ZENODO_SANDBOX)
    try:
        z.update(ZENODO_DEPOSITION_ID, paths)
    except requests.exceptions.HTTPError as e:
        if 'errors' in e.response:
            print(e.response['errors'], file=sys.stderr)
        raise e

def archiveToZenodo(tmpdir, max_retries: int, sleep_seconds: int = 60):
    with tempfile.TemporaryDirectory() as zpath:
        zpath = Path(zpath)
        shutil.make_archive(zpath / 'archive', 'zip', tmpdir)

        i: int = 0
        success: bool = False
        while i < max_retries and not success:
            try:
                uploadToZenodo([zpath / 'archive.zip'])
                success = True
            except requests.exceptions.HTTPError as e:
                if e.errno == 504:
                    print(f"Retrying upload to Zenodo {i}/{max_retries}")
                    sleep(sleep_seconds)
                else:
                    raise e
            i += 1

def uploadToKaggle(path, version_notes):
    from kaggle import api as k
    
    k.dataset_create_version(path, version_notes, dir_mode='zip')

def archiveToKaggle(tmpdir):
    with tempfile.TemporaryDirectory() as kpath:
        kpath = Path(kpath)
        shutil.copytree(tmpdir, kpath, dirs_exist_ok=True)

        with open(kpath / 'dataset-metadata.json', 'w') as md:
            json.dump({
                "id": "daviddavo/dao-analyzer",
            }, md)

        with open(kpath / 'update_date.txt', 'r') as ud:
            update_date = ud.readline()
        
        uploadToKaggle(kpath, update_date)

def main():
    import argparse

    parser = argparse.ArgumentParser("Update datawarehouse in Kaggle and Zenodo")

    available_repos = ['zenodo', 'kaggle']
    parser.add_argument(
        'repos',
        nargs='*',
        default='all',
        choices=[*available_repos, 'all'],
        help="Which repositories to upload the data",
    )
    parser.add_argument(
        '-z',
        '--zenodo-max-retries',
        type=int,
        default=5,
        help="Zenodo is known to return 504 error, this program will try and upload it again",
    )

    args = parser.parse_args() 
    if args.repos == 'all':
        args.repos = available_repos

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        print("Archiving datawarehouse")
        archivedw(DEFAULT_DATAWAREHOUSE, tmpdir)
        if 'zenodo' in args.repos:
            print("Uploading to zenodo")
            archiveToZenodo(tmpdir, args.zenodo_max_retries)
        if 'kaggle' in args.repos:
            print("Uploading to kaggle")
            archiveToKaggle(tmpdir)

if __name__ == '__main__':
    main()

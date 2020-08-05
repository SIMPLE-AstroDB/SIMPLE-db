#!/usr/bin/env python
"""
Prints the info about the object in this div
"""
# inbuilt libs
import json

# local libs
from web_base import load_db, target_exists, target_argparse


def output(inventory: dict):
    del inventory['Photometry']
    inventory = json.dumps(inventory, indent=4)
    outstr = f"""
        document.getElementById("objectInfo").innerText = '';
        document.getElementById("objectInfo").textContent = {inventory};
    """
    return inventory, outstr


def main(target: str):
    db = load_db()
    inventory = target_exists(db, target)
    inventory = output(inventory)[0]
    return inventory


if __name__ == '__main__' or 'bokeh' in __name__:
    _tgt = target_argparse()
    main(_tgt)

#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, logging.config
from pathlib import Path

from .argtype import ArgType
from .pipeline import Pipeline
from .ifunc import IFunc

LOG_CONF_FILE = os.environ.get('LOG_CONF_FILE', None)
LOG_DIR = os.environ.get("LOG_DIR", "")

if LOG_CONF_FILE is not None and os.path.exists(LOG_CONF_FILE):
    from ruamel.yaml import YAML
    if not os.path.exists(LOG_DIR):
        Path(LOG_DIR).mkdir(exist_ok=True, parents=True)

    with open(LOG_CONF_FILE, "r") as f:
        yaml = YAML()
        data = yaml.load(f)

        for handler in data.get('handlers', []).values():
            if 'filename' in handler:
                handler['filename'] = os.path.abspath(os.path.join(LOG_DIR, handler['filename']))

        logging.config.dictConfig(data)
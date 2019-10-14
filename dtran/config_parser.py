from pathlib import Path
from typing import Union

from dtran import Pipeline


class ConfigParser:
    def __init__(self):
        pass

    def parse(self, path: Union[Path, str]) -> Pipeline:
        pass

    def _parse_from_yaml(self, path: Union[Path, str]) -> Pipeline:
        pass

    def _parse_from_json(self, path: Union[Path, str]) -> Pipeline:
        pass

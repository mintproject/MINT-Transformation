import re
import ujson
from ruamel.yaml import YAML
from pathlib import Path
from typing import Union, Tuple, Dict
from marshmallow import Schema, fields, validate, ValidationError, post_load
from collections import OrderedDict, defaultdict
from importlib import import_module

from dtran import Pipeline

wired_pattern = re.compile(r'^\$\..+\..+$')
keys_pattern = re.compile(r'^\w+$')

class OrderedDictField(fields.Dict):
    mapping_type = OrderedDict

class AdapterSchema(Schema):
    adapter = fields.Str(required=True)
    comment = fields.Str()
    inputs = OrderedDictField(keys=fields.Str(validate=validate.Regexp(keys_pattern)), values=fields.Raw())

    class Meta:
        ordered = True

class PipelineSchema(Schema):
    version = fields.Str(required=True)
    description = fields.Str()
    adapters = OrderedDictField(required=True, validate=validate.Length(min=1),
                                keys=fields.Str(validate=validate.Regexp(keys_pattern)),
                                values=fields.Nested(AdapterSchema()))

    class Meta:
        ordered = True

    @post_load
    def construct_pipeline(self, data, **kwargs):
        adapter_count = defaultdict(int)
        mappings = {}
        for name, adapter in data['adapters'].items():
            mod, *cls = adapter['adapter'].rsplit('.')
            try:
                mod = import_module(mod)
                cls = getattr(mod, cls[0])
            except (ModuleNotFoundError, IndexError, AttributeError):
                raise ValidationError(f"Invalid adapter {adapter['adapter']} for {name}")
            adapter_count[adapter['adapter']] += 1
            mappings[name] = (cls, adapter_count[adapter['adapter']])
        inputs = {}
        wired = []
        func_classes = []
        for name, adapter in data['adapters'].items():
            func_classes.append(mappings[name][0])
            if 'inputs' not in adapter:
                continue
            for input, value in adapter['inputs'].items():
                if isinstance(value, str) and wired_pattern.match(value):
                    adapter_name, output = value.split('.')[1:]
                    if adapter_name not in mappings:
                        raise ValidationError(f"Invalid adapter name {adapter_name} in input {input} for {name}")
                    wired.append(
                        getattr(getattr(mappings[adapter_name][0].O, f"_{mappings[adapter_name][1]}"), output)
                        == getattr(getattr(mappings[name][0].I, f"_{mappings[name][1]}"), input)
                    )
                else:
                    inputs[f"{mappings[name][0].id}__{mappings[name][1]}__{input}"] = value
        for name in mappings:
            mappings[name] = f"{mappings[name][0].id}__{mappings[name][1]}__"
        return Pipeline(func_classes, wired), inputs, mappings

class ConfigParser:
    def __init__(self):
        self.schema = PipelineSchema()

    def parse(self, path: Union[Path, str]) -> Tuple[Pipeline, Dict, Dict]:
        path = str(path)
        if path.endswith('.json'):
            return self._parse_from_json(path)
        elif path.endswith('.yml') or path.endswith('.yaml'):
            return self._parse_from_yaml(path)
        raise Exception(f"Unsupported file format for{path}. Only supports json or yaml file")

    def _parse_from_yaml(self, path: Union[Path, str]) -> Tuple[Pipeline, Dict, Dict]:
        yaml = YAML()
        with open(str(path), 'r') as file:
            return self.schema.load(yaml.load(file))

    def _parse_from_json(self, path: Union[Path, str]) -> Tuple[Pipeline, Dict, Dict]:
        with open(str(path), 'r') as file:
            return self.schema.load(ujson.load(file))

import re
from collections import OrderedDict, defaultdict
from importlib import import_module
from pathlib import Path
from typing import Union, Tuple, Dict

import ujson
from marshmallow import Schema, fields, validate, ValidationError, post_load
from ruamel.yaml import YAML

from dtran import Pipeline
from dtran.ifunc import IFunc
from dtran.wireio import WiredIOArg

wired_pattern = re.compile(r'^\$\.\w+\.\w+$')
inputs_pattern = re.compile(r'^\$\$\.\w+$')
keys_pattern = re.compile(r'^\w+$')


class OrderedDictField(fields.Dict):
    mapping_type = OrderedDict


class AdapterSchema(Schema):
    adapter = fields.Str(required=True)
    comment = fields.Str()
    inputs = OrderedDictField(keys=fields.Str(validate=validate.Regexp(keys_pattern)), values=fields.Raw(allow_none=True))

    class Meta:
        ordered = True


class PipelineSchema(Schema):
    def __init__(self, cli_inputs, **kwargs):
        super().__init__(**kwargs)
        self.cli_inputs = cli_inputs

    version = fields.Str(required=True)
    description = fields.Str()
    inputs = OrderedDictField(validate=validate.Length(min=1),
                              keys=fields.Str(validate=validate.Regexp(keys_pattern)),
                              values=fields.Raw(allow_none=True))
    adapters = OrderedDictField(required=True, validate=validate.Length(min=1),
                                keys=fields.Str(validate=validate.Regexp(keys_pattern)),
                                values=fields.Nested(AdapterSchema()))

    class Meta:
        ordered = True

    @staticmethod
    def process_input(val, data):
        # processing for root-level pipeline inputs recursively
        if isinstance(val, str):
            if not inputs_pattern.match(val):
                return val
            input_name = val.split('.')[1:][0]
            if ('inputs' not in data) or (input_name not in data['inputs']):
                raise ValidationError(f"invalid pipeline input {input_name}")
            return data['inputs'][input_name]
        if isinstance(val, dict):
            inputs = val.items()
        else:
            inputs = enumerate(val)
        for input, value in inputs:
            if isinstance(value, str) or isinstance(value, dict) or isinstance(value, list):
                val[input] = PipelineSchema.process_input(value, data)
        return val

    @post_load
    def construct_pipeline(self, data, **kwargs):
        adapter_count = defaultdict(int)
        # map from custom function name to adapter class and order eg. {'MyCutomName1': (<class 'funcs.readers.read_func.ReadFunc'>, 1)}
        mappings = {}
        for name, adapter in data['adapters'].items():
            # separating module path and adapter class
            mod, *cls = adapter['adapter'].rsplit('.', 1)
            try:
                mod = import_module(mod)
            except Exception as e:
                raise ValidationError([str(e), f"could not import adapter {adapter['adapter']} for {name}"])
            try:
                cls = getattr(mod, cls[0])
            except AttributeError as e:
                raise ValidationError([str(e), f"could not import adapter {adapter['adapter']} for {name}"])
            except IndexError:
                raise ValidationError(f"missing adapter class in {adapter['adapter']} for {name}")
            if not issubclass(cls, IFunc):
                raise ValidationError(f"invalid adapter class {adapter['adapter']} for {name}")
            adapter_count[adapter['adapter']] += 1
            mappings[name] = (cls, adapter_count[adapter['adapter']])

        # validating cli_inputs
        for name, input in self.cli_inputs:
            if name not in mappings:
                raise ValidationError(
                    ['cli_inputs exception', f"invalid adapter name {name}. not found in config file"])
            if input not in mappings[name][0].inputs:
                raise ValidationError(['cli_inputs exception',
                                       f"invalid input {input} in {data['adapters'][name]['adapter']} for {name}"])
            # cli_inputs has higher priority and overwrites config_file data
            if 'inputs' not in data['adapters'][name]:
                data['adapters'][name]['inputs'] = OrderedDict()
            data['adapters'][name]['inputs'][input] = self.cli_inputs[(name, input)]

        inputs = {}
        wired = []
        func_classes = []
        # processing data and populating inputs
        for name, adapter in data['adapters'].items():
            func_classes.append(mappings[name][0])
            if 'inputs' not in adapter:
                continue
            for input, value in adapter['inputs'].items():
                # validating input
                if input not in mappings[name][0].inputs:
                    raise ValidationError(f"invalid input {input} in {data['adapters'][name]['adapter']} for {name}")
                # processing for root-level pipeline inputs
                if isinstance(value, str) or isinstance(value, dict) or isinstance(value, list):
                    try:
                        value = PipelineSchema.process_input(value, data)
                    except ValidationError as e:
                        raise ValidationError(f"{e} in {input} for {name}")
                # skipping inputs with null values
                if value is None:
                    continue
                # processing wiring inputs
                if isinstance(value, str) and wired_pattern.match(value):
                    adapter_name, output = value.split('.')[1:]
                    # validating wired input
                    if adapter_name not in mappings:
                        raise ValidationError(f"invalid adapter name {adapter_name} in input {input} for {name}")
                    elif output not in mappings[adapter_name][0].outputs:
                        raise ValidationError(f"invalid output {output} of {adapter_name} in input {input} for {name}")
                    wired.append(
                        getattr(getattr(mappings[adapter_name][0].O, f"_{mappings[adapter_name][1]}"), output)
                        == getattr(getattr(mappings[name][0].I, f"_{mappings[name][1]}"), input)
                    )
                # processing other inputs
                else:
                    argtype = mappings[name][0].inputs[input]
                    # type casting input from string to required argtype
                    if isinstance(value, str):
                        try:
                            value = argtype.type_cast(value)
                        except (NotImplementedError, ValueError) as e:
                            raise ValidationError([str(e), f"type casting failed in input {input} for {name}"])
                    inputs[WiredIOArg.get_arg_name(mappings[name][0].id, mappings[name][1], input)] = value

        return Pipeline(func_classes, wired), inputs


class ConfigParser:
    def __init__(self, cli_inputs: Dict = None):
        self.schema = PipelineSchema(cli_inputs or {})

    def parse(self, path: Union[Path, str] = None, conf_obj: Dict = None) -> Tuple[Pipeline, Dict]:
        if path is not None:
            path = str(path)
            if path.endswith('.json'):
                return self._parse_from_json(path)
            elif path.endswith('.yml') or path.endswith('.yaml'):
                return self._parse_from_yaml(path)
            raise Exception(f"Unsupported file format for{path}. Only supports json or yaml file")
        else:
            if conf_obj is not None:
                return self.schema.load(conf_obj)
            else:
                raise ValueError("Config parser should take in at least one valid param!")

    def _parse_from_yaml(self, path: Union[Path, str]) -> Tuple[Pipeline, Dict]:
        yaml = YAML()
        with open(str(path), 'r') as file:
            return self.schema.load(yaml.load(file))

    def _parse_from_json(self, path: Union[Path, str]) -> Tuple[Pipeline, Dict]:
        with open(str(path), 'r') as file:
            return self.schema.load(ujson.load(file))

import json
import os

from flask import Blueprint, jsonify

import funcs
from dtran.ifunc import IFuncType

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'
KEY_FRIENDLY_NAME = "friendly_name"
KEY_FUNC_TYPE = "func_type"
KEY_EXAMPLE = "example"


FAKE_ADAPTERS = [
    {
        "id": "MosaickTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Mosaick Transformation",
        "friendly_name": "Generate tiles to cover larger bounding box (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "ClippingTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Clipping Transformation",
        "friendly_name": "Clip to a smaller bounding box (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "ResampleTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Resampling Transformation",
        "friendly_name": "Resample to change spatial resolution (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "StackTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Stacking Transformation",
        "friendly_name": "Stack grids for different times into a single file (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "ExtractTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Extraction Transformation",
        "friendly_name": "Extract derived grid from grids (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "RegridTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Regridding Transformation",
        "friendly_name": "Wrap values to different computational grid (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "ReprojectTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Reprojection Transformation",
        "friendly_name": "Reproject to a different map (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "RescaleTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Rescaling Transformation",
        "friendly_name": "Change all values by one factor (WIP)",
        "example": {},
        "is_fake": True
    },
    {
        "id": "VoidFillingTrans",
        "description": "",
        "inputs": {},
        "outputs": {},
        "func_type": "Void Filling Transformation",
        "friendly_name": "Replace all nodata values (WIP)",
        "example": {},
        "is_fake": True
    },
]

class AdapterElement:
    """ An adapter element includes attributes needed to contrcut a pipeline.
    Also used for browsing the Data Transformation Adapters Catalog. """

    @classmethod
    def from_json(cls, json_object):
        """ Create an AdapterElement class from a json_object. """
        name = json_object['name']
        module_type = json_object['module_type']
        identifier = json_object['identifier']
        description = json_object['description']
        inputs = json_object['inputs']
        outputs = json_object['outputs']
        friendly_name = json_object['friendly_name']
        func_type = json_object['func_type']
        example = json_object['example']

        return cls(
            name, module_type, identifier, description, inputs, outputs,
            friendly_name, func_type, example
        )

    def __init__(
            self, name, module_type, identifier, description, inputs,
            outputs, friendly_name, func_type, example
    ):
        """ Initialize AdapterElement. """
        self.name = name
        self.module_type = module_type
        self.identifier = identifier
        self.description = description
        self.inputs = inputs
        self.outputs = outputs
        self.friendly_name = friendly_name
        self.func_type = func_type
        self.example = example

    def __repr__(self):
        """ Print AdapterElement. """
        return f'name={self.name}, module_type={self.module_type}, ' + \
               f'identifier={self.identifier}\ndescription={self.description}\n' + \
               f'inputs={self.inputs}\noutputs={self.outputs}\n\n' + \
               f'func_type={self.func_type}\nfriendly_name={self.friendly_name}' + \
               f'example={self.example}'

    def get_adapter_identifier(self):
        """ Get AdapterElement identifier. """
        return self.identifier

    def get_adapter_name(self):
        """ Get AdapterElement name. """
        return self.name

    def get_adapter_inputs(self):
        """ Get AdapterElement inputs. """
        return self.inputs

    def get_adapter_outputs(self):
        """ Get AdapterElement outputs. """
        return self.outputs

    def get_adapter_description(self):
        """ Get AdapterElement description. """
        return self.description

    def get_adapter_func_type(self):
        """ Get AdapterElement func_type. """
        return self.func_type

    def get_adapter_friendly_name(self):
        """ Get AdapterElement friendly_name. """
        return self.friendly_name

    def get_adapter_example(self):
        return self.example


class AdapterDB:
    """ The AdapterDB is the Data Transformation Adapters Catalog.
    It holds instances of AdapterElement class. """

    def __init__(self):
        """ Initialize AdapterDB. """
        self.adapters = list()
        self.name2object = dict()
        self.initialize_adapters()

    def get_adapter_object_from_name(self, adp_name):
        """ Get an Adapter object (class) from the name of the adapter. """
        if adp_name in self.name2object:
            return self.name2object[adp_name]
        return None

    def initialize_adapters(self):
        """ Initialize adapters in AdapterDB by reading the 'funcs' python module. """
        for a_name, a_cls in funcs.__dict__.items():
            if isinstance(a_cls, type):
                cls_dict    = a_cls.__dict__
                module_type = cls_dict[KEY_MODL].split('.')[-1]
                identifier  = cls_dict[KEY_IDENTIFIER]

                # Get description
                description = None
                if KEY_DESC in cls_dict:
                    description = cls_dict[KEY_DESC]

                # Get function type
                func_type = IFuncType.OTHERS.value
                if KEY_FUNC_TYPE in cls_dict:
                    func_type = cls_dict[KEY_FUNC_TYPE].value

                # Get friendly name
                friendly_name = None
                if KEY_FRIENDLY_NAME in cls_dict:
                    friendly_name = cls_dict[KEY_FRIENDLY_NAME]

                # Get inputs/outputs
                inputs_dict  = cls_dict[KEY_INPUTS]
                outputs_dict = cls_dict[KEY_OUTPUTS]
                inputs, outputs = dict(), dict()
                for arg_name, arg_attr in inputs_dict.items():
                    inputs[arg_name] = {'id': arg_attr.__dict__['id'], \
                        'val': arg_attr.__dict__['val'], 'optional': arg_attr.__dict__['optional']}
                for arg_name, arg_attr in outputs_dict.items():
                    outputs[arg_name] = {'id': arg_attr.__dict__['id'], \
                        'val': arg_attr.__dict__['val'], 'optional': arg_attr.__dict__['optional']}

                # Get example
                example = {}
                if KEY_EXAMPLE in cls_dict:
                    example = cls_dict[KEY_EXAMPLE]

                self.adapters.append(AdapterElement(
                    a_name, module_type, identifier, description, inputs,
                    outputs, friendly_name, func_type, example
                ))
                self.name2object[a_name] = a_cls

    def get_list_of_adapters(self):
        """ Get the list of AdapterElements in the AdapterDB. """
        return self.adapters

    def get_list_of_adapter_names_for_dropdown(self):
        """ Get the list of AdapterElements names (used for dropdown in the main menu). """
        adapter_names_list = list()
        for adp_idx, adp_ins in enumerate(self.get_list_of_adapters()):
            adapter_names_list.append(f'{adp_idx} {adp_ins.identifier}/{adp_ins.name}')
        return adapter_names_list


def setup_adapters():
    if os.path.isfile('./webapp/adapters.json'):
        print("adapters.json already exists! Removing it...")
        os.system("rm ./webapp/adapters.json")
    adapterdb = AdapterDB()
    with open("./webapp/adapters.json", "w") as f:
        adapter_list = adapterdb.get_list_of_adapters()
        # TODO: dump in fake adapters as well
        results = [{
            "id": ad.get_adapter_name(),
            "description": ad.get_adapter_description(),
            "inputs": ad.get_adapter_inputs(),
            "outputs": ad.get_adapter_outputs(),
            "func_type": ad.get_adapter_func_type(),
            "friendly_name": ad.get_adapter_friendly_name(),
            "example": ad.get_adapter_example()
        } for ad in adapter_list]
        results = results + FAKE_ADAPTERS
        json.dump(results, f, indent=4)
    print("Finish loading in all adapters!")


setup_adapters()

adapters_blueprint = Blueprint("adapters", "adapters", url_prefix="/api")


@adapters_blueprint.route('/adapters', methods=["GET"])
def list_adapters():
    # TODO: put adapters in db?
    with open("./webapp/adapters.json", "r") as f:
        adapters = json.load(f)
    return jsonify(adapters)




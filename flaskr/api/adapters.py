from flask import Blueprint, jsonify
import json
import os
import funcs
from dtran.ifunc import IFuncType

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'
KEY_FRIENDLY_NAME = "friendly_name"
KEY_FUNC_TYPE = "func_type"


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

        return cls(name, module_type, identifier, description, inputs, outputs, friendly_name, func_type)

    def __init__(self, name, module_type, identifier, description, inputs, outputs, friendly_name, func_type):
        """ Initialize AdapterElement. """
        self.name = name
        self.module_type = module_type
        self.identifier = identifier
        self.description = description
        self.inputs = inputs
        self.outputs = outputs
        self.friendly_name = friendly_name
        self.func_type = func_type

    def __repr__(self):
        """ Print AdapterElement. """
        return f'name={self.name}, module_type={self.module_type}, ' + \
               f'identifier={self.identifier}\ndescription={self.description}\n' + \
               f'inputs={self.inputs}\noutputs={self.outputs}\n\n' + \
               f'func_type={self.func_type}\nfriendly_name={self.friendly_name}'

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
                description = None
                if KEY_DESC in cls_dict:
                    description = cls_dict[KEY_DESC]
                func_type = IFuncType.OTHERS.value
                if KEY_FUNC_TYPE in cls_dict:
                    func_type = cls_dict[KEY_FUNC_TYPE].value
                friendly_name = None
                if KEY_FRIENDLY_NAME in cls_dict:
                    friendly_name = cls_dict[KEY_FRIENDLY_NAME]
                inputs_dict  = cls_dict[KEY_INPUTS]
                outputs_dict = cls_dict[KEY_OUTPUTS]
                inputs, outputs = dict(), dict()
                for arg_name, arg_attr in inputs_dict.items():
                    inputs[arg_name] = {'id': arg_attr.__dict__['id'], \
                        'val': arg_attr.__dict__['val'], 'optional': arg_attr.__dict__['optional']}
                for arg_name, arg_attr in outputs_dict.items():
                    outputs[arg_name] = {'id': arg_attr.__dict__['id'], \
                        'val': arg_attr.__dict__['val'], 'optional': arg_attr.__dict__['optional']}

                self.adapters.append(AdapterElement(
                    a_name, module_type, identifier, description, inputs, outputs, friendly_name, func_type
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
    if not os.path.isfile('./adapters.json'):
        # generate adapter.json on the fly
        adapterdb = AdapterDB()
        with open("./adapters.json", "w") as f:
            adapter_list = adapterdb.get_list_of_adapters()
            json.dump([{
                "id": ad.get_adapter_name(),
                "description": ad.get_adapter_description(),
                "inputs": ad.get_adapter_inputs(),
                "outputs": ad.get_adapter_outputs(),
                "func_type": ad.get_adapter_func_type(),
                "friendly_name": ad.get_adapter_friendly_name()
            } for ad in adapter_list], f, indent=4)
    else:
        print("adapters.json already exists!")


setup_adapters()

adapters_blueprint = Blueprint("adapters", "adapters", url_prefix="/api")


@adapters_blueprint.route('/adapters', methods=["GET"])
def list_adapters():
    # TODO: put adapters in db?
    with open("./adapters.json", "r") as f:
        adapters = json.load(f)
    return jsonify(adapters)




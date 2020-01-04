from flask import Blueprint, jsonify
import funcs

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'


adapters_blueprint = Blueprint("adapters", "adapters", url_prefix="/api")


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

        # update local list of graph instances when loading from JSON file
        # update_list_of_session_graphs_from_args_dict(inputs)
        # update_list_of_session_graphs_from_args_dict(outputs)

        return cls(name, module_type, identifier, description, inputs, outputs)

    def __init__(self, name, module_type, identifier, description, inputs, outputs):
        """ Initialize AdapterElement. """
        self.name = name
        self.module_type = module_type
        self.identifier = identifier
        self.description = description
        self.inputs = inputs
        self.outputs = outputs

    def __repr__(self):
        """ Print AdapterElement. """
        return f'name={self.name}, module_type={self.module_type}, ' + \
               f'identifier={self.identifier}\ndescription={self.description}\n' + \
               f'inputs={self.inputs}\noutputs={self.outputs}\n\n'

    def get_adapter_identifier(self):
        """ Get AdapterElement identifier. """
        return self.identifier

    def get_adapter_name(self):
        """ Get AdapterElement name. """
        return self.name

    def get_adapter_description(self):
        return self.description

    def get_adapter_inputs(self):
        return self.inputs

    def get_adapter_outputs(self):
        return self.outputs


class AdapterDB:
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
            # print(a_name)
            if isinstance(a_cls, type):
                cls_dict = a_cls.__dict__
                module_type = cls_dict[KEY_MODL].split('.')[-1]
                identifier = cls_dict[KEY_IDENTIFIER]
                description = None
                if KEY_DESC in cls_dict:
                    description = cls_dict[KEY_DESC]
                inputs_dict = cls_dict[KEY_INPUTS]
                outputs_dict = cls_dict[KEY_OUTPUTS]
                inputs, outputs = dict(), dict()
                for arg_name, arg_attr in inputs_dict.items():
                    inputs[arg_name] = {
                        'id': arg_attr.__dict__['id'],
                        'val': arg_attr.__dict__['val'],
                        'optional': arg_attr.__dict__['optional']
                    }
                for arg_name, arg_attr in outputs_dict.items():
                    outputs[arg_name] = {
                        'id': arg_attr.__dict__['id'],
                        'val': arg_attr.__dict__['val'],
                        'optional': arg_attr.__dict__['optional']
                    }
                self.adapters.append(AdapterElement(a_name, module_type, identifier, description, inputs, outputs))
                self.name2object[a_name] = a_cls

    def get_list_of_adapters(self):
        """ Get the list of AdapterElements in the AdapterDB. """
        return self.adapters


adapter_db = AdapterDB()


@adapters_blueprint.route('/adapters', methods=["GET"])
def list_adapters():
    adapters_list = adapter_db.get_list_of_adapters()
    return jsonify([{
        "name": adpt.get_adapter_name(),
        "func_name": adpt.get_adapter_name(),
        "description": adpt.get_adapter_description(),
        "input": adpt.get_adapter_inputs(),
        "ouput": adpt.get_adapter_outputs()
    } for adpt in adapters_list])

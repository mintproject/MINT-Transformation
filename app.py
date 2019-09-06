from flask import Flask, render_template, request
from copy import deepcopy
import funcs
from funcs import *
from dtran import Pipeline
from json import dumps, JSONEncoder, JSONDecoder

app = Flask(__name__)

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'

FORM_ADPTR_SLCTD = 'slct_adp'
FORM_ADPTR_SUBMT = 'add_adp'
FORM_ADPTR_RMV   = 'remove_from_pipe'
FORM_PIP_UPDT = 'update_pipe'
FORM_PIP_CLR = 'clear_pipe'
FORM_PIP_EXE  = 'exe_pipe'
FORM_PIP_LOAD = 'load_pipe'
FORM_PIP_SAVE = 'save_pipe'
FORM_PIP_FILE_LOAD = 'pipe_config_load_file'
FORM_PIP_FILE_SAVE = 'pipe_config_save_file'

GRAPH_INST_W_REPR  = 'GraphInstanceWire'
GRAPH_INST_ADD     = 'Create graph instance'
GRAPH_INST_CHANGE  = '__select'
# TODO: support NumPy

# --- helpers -----------------------------------------------------------------

class CustomEncoder(JSONEncoder):
    ''' Custom encoder used serialize dictionaries and classes (i.e. AdapterElement). '''

    def default(self, o):
        return {'__{}__'.format(o.__class__.__name__): o.__dict__}

def AdapterElement_from_json(json_obj):
    ''' Custom decoder used to deserialize AdapterElement class. '''

    adp_el_str = '__AdapterElement__'
    if adp_el_str in json_obj:
        return AdapterElement.from_json(json_obj[adp_el_str])
    else:
        return json_obj

def update_list_of_global_graphs_from_args_dict(args_dict):
    ''' Update the list of g_graphs_dropdown_list based on a given arguments dictionary.
    This method is called when loading a pipeline file externally. '''

    global g_graphs_dropdown_list

    for _, item in args_dict.items():
        if item['id'] == 'graph' and item['val'] not in g_graphs_dropdown_list:
            g_graphs_dropdown_list.append(item['val'])

def init_list_of_global_graphs():
    ''' Initialize the list of g_graphs_dropdown_list. '''

    global g_graphs_dropdown_list

    g_graphs_dropdown_list = ['', GRAPH_INST_ADD]

def was_graph_instance_added(req_args_list):
    ''' Check whether one of the element in a given list includes GRAPH_INST_CHANGE in name. '''

    # iterate over list
    for arg_name in req_args_list:
        if GRAPH_INST_CHANGE in arg_name:
            return True
    return False

# --- classes -----------------------------------------------------------------

class AdapterElement:
    ''' An adapter element includes attributes needed to contrcut a pipeline.
    Also used for browsing the Data Transformation Adapters Catalog. '''

    @classmethod
    def from_json(cls, json_object):
        ''' Create an AdapterElement class from a json_object. '''

        name = json_object['name']
        module_type = json_object['module_type']
        identifier = json_object['identifier']
        description = json_object['description']
        inputs = json_object['inputs']
        outputs = json_object['outputs']

        # update global list of graph instances when loading from JSON file
        update_list_of_global_graphs_from_args_dict(inputs)
        update_list_of_global_graphs_from_args_dict(outputs)

        ret_class = cls(name, module_type, identifier, description, inputs, outputs)
        return ret_class

    def __init__(self, name, module_type, identifier, description, inputs, outputs):
        ''' Initialize AdapterElement. '''

        self.name = name
        self.module_type = module_type
        self.identifier = identifier
        self.description = description
        self.inputs = inputs
        self.outputs = outputs
    
    def __repr__(self):
        ''' Print AdapterElement. '''

        return f'name={self.name}, module_type={self.module_type}, ' + \
            f'identifier={self.identifier}\ndescription={self.description}\n' + \
            f'inputs={self.inputs}\noutputs={self.outputs}\n\n'

    def get_adapter_identifier(self):
        ''' Get AdapterElement identifier. '''

        return self.identifier

    def get_adapter_name(self):
        ''' Get AdapterElement name. '''

        return self.name

class AdapterDB:
    ''' The AdapterDB is the Data Transformation Adapters Catalog.
    It holds instances of AdapterElement class. '''

    def __init__(self):
        ''' Initialize AdapterDB. '''

        self.adapters = list()
        self.name2object = dict()
        self.initialize_adapters()

    def get_adapter_object_from_name(self, adp_name):
        ''' Get an Adapter object (class) from the name of the adapter. '''

        if adp_name in self.name2object:
            return self.name2object[adp_name]
        return None

    def initialize_adapters(self):
        ''' Initialize adapters in AdapterDB by reading the 'funcs' python module. '''

        for a_name, a_cls in funcs.__dict__.items():
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
                    inputs[arg_name] = {'id': arg_attr.__dict__['id'], \
                        'val': arg_attr.__dict__['val'], 'optional': arg_attr.__dict__['optional']}
                for arg_name, arg_attr in outputs_dict.items():
                    outputs[arg_name] = {'id': arg_attr.__dict__['id'], \
                        'val': arg_attr.__dict__['val'], 'optional': arg_attr.__dict__['optional']}

                self.adapters.append(AdapterElement(a_name, module_type, identifier, description, inputs, outputs))
                self.name2object[a_name] = a_cls

    def get_list_of_adapters(self):
        ''' Get the list of AdapterElements in the AdapterDB. '''

        return self.adapters

    def get_list_of_adapter_names_for_dropdown(self):
        ''' Get the list of AdapterElements names (used for dropdown in the main menu). '''

        adapter_names_list = list()
        for adp_idx, adp_ins in enumerate(self.get_list_of_adapters()):
            adapter_names_list.append(f'{adp_idx} {adp_ins.identifier}/{adp_ins.name}')
        return adapter_names_list

# --- pipeline methods --------------------------------------------------------

def get_next_index_in_g_pipeline():
    global g_pipeline

    if len(g_pipeline) == 0:
        return 0
    else:
        return g_pipeline[-1][0] + 1

def remove_adapter_from_pipeline(adapter_identifier_in_pipe):
    global g_pipeline
    for index_in_list, adapter_tuple in enumerate(g_pipeline):
        if adapter_identifier_in_pipe == adapter_tuple[0]:
            index_to_access = index_in_list
            break
    g_pipeline.pop(index_to_access)

def update_g_pipeline_elements(adapter_identifier_in_pipe, input_n_output, adapter_attribute, value):
    global g_pipeline

    ''' g_pipeline is a list
        ...[adapter_identifier_in_pipe] is an element in the pipe
        ...[0] holds the index, ...[1] hold the instance of the adapter '''

    for index_in_list, adapter_tuple in enumerate(g_pipeline):
        if adapter_identifier_in_pipe == adapter_tuple[0]:
            index_to_access = index_in_list
            break

    # inputs/outputs hold a dictionary of attributes, each one is a dictionary by itself
    if input_n_output:
        g_pipeline[index_to_access][1].inputs[adapter_attribute]['val'] = value
    else:
        g_pipeline[index_to_access][1].outputs[adapter_attribute]['val'] = value

def execute_pipeline():
    global g_pipeline, g_adapterdb

    inputs = {}
    pipeline_classes = []
    pipeline_wires = []

    graph_instncs_dict = dict()

    for _, adapter in g_pipeline:
        adptr_name  = adapter.get_adapter_name()
        adptr_obj = g_adapterdb.get_adapter_object_from_name(adptr_name)
        adptr_id  = adapter.get_adapter_identifier()
        pipeline_classes.append(adptr_obj)
        # parse inputs
        for adp_inp_key, adp_inp_dict in adapter.inputs.items():
            adp_inp_val = adp_inp_dict['val']
            if adp_inp_val != '' and adp_inp_val != None:
                # explicit input
                if GRAPH_INST_W_REPR not in adp_inp_val:
                    # TODO: this is a temporary workaround... fix and parse according to ArgType
                    if adp_inp_val == '{}':
                        adp_inp_val = {}
                    # TODO: solve issue with index (it's hard coded to 1 now!) in f"{id}__{idx}__{argname}"
                    inputs[f'{adptr_id}__1__{adp_inp_key}'] = adp_inp_val
                else: # graph/numpy
                    if adp_inp_val not in graph_instncs_dict:
                        graph_instncs_dict[adp_inp_val] = dict()
                    if 'i' not in graph_instncs_dict[adp_inp_val]:
                        graph_instncs_dict[adp_inp_val]['i'] = list()
                    graph_instncs_dict[adp_inp_val]['i'].append( [adptr_id, None, adp_inp_key] )
        # parse outputs
        for adp_out_key, adp_out_dict in adapter.outputs.items():
            adp_out_val = adp_out_dict['val']
            if adp_out_val != '' and adp_out_val != None and GRAPH_INST_W_REPR in adp_out_val:
                if adp_out_val not in graph_instncs_dict:
                    graph_instncs_dict[adp_out_val] = dict()
                if 'o' not in graph_instncs_dict[adp_out_val]:
                    graph_instncs_dict[adp_out_val]['o'] = list()
                graph_instncs_dict[adp_out_val]['o'].append( [adptr_id, None, adp_out_key] )

    '''
    'graph_instance_name' -->
        'input'  --> list( (adp_identifier, None, adp_input_key ) )
        'output' --> list( (adp_identifier, None, adp_output_key) )
    '''
    for graph_inst_key, graph_inst_list in graph_instncs_dict.items():
        # TODO: support more than 1 input and 1 output of specific instance
        wire = ( graph_inst_list['i'][0], graph_inst_list['o'][0] )
        pipeline_wires.append(wire)
    
    # Pipeline Object
    pipeline = Pipeline(pipeline_classes, wired=pipeline_wires)
    # TODO: print to screen and show errors/results
    pipeline.exec(inputs)

# --- entrypoints -------------------------------------------------------------

@app.route('/pipeline', methods=['GET', 'POST'])
def pipeline():
    global g_pipeline, g_adapterdb, g_graphs_dropdown_list

    # get list of adapters and their names
    list_of_adapters = g_adapterdb.get_list_of_adapters()
    list_of_adapter_names = g_adapterdb.get_list_of_adapter_names_for_dropdown()
    pip_exe_msg = ""

    # set default option
    adp_id_str_chosen = list_of_adapter_names[0]
    # show selected adapter
    if FORM_ADPTR_SLCTD in request.args:
        adp_id_str_chosen = request.args.get(FORM_ADPTR_SLCTD)
    adp = list_of_adapters[int(adp_id_str_chosen.split(' ')[0])]

    # check if user loaded a pipeline config file
    if FORM_PIP_LOAD in request.args and \
        FORM_PIP_FILE_LOAD in request.args and \
        request.args[FORM_PIP_FILE_LOAD] != '': 

        g_pipeline.clear()
        init_list_of_global_graphs()

        # load a pipeline configuration from file
        input_config_file = request.args[FORM_PIP_FILE_LOAD]
        with open(input_config_file, 'r') as read_file:
            for line_i, line_d in enumerate(read_file):
                adapter_el = JSONDecoder(object_hook = AdapterElement_from_json).decode(line_d)
                load_adapter = (line_i, adapter_el)
                g_pipeline.append(load_adapter)

    elif FORM_PIP_SAVE in request.args and \
        FORM_PIP_FILE_SAVE in request.args and \
        request.args[FORM_PIP_FILE_SAVE] != '': # or save a pipeline config file

        # save a pipeline configuration to file
        output_config_file = request.args[FORM_PIP_FILE_SAVE]
        with open(output_config_file, 'w') as write_file:
            for pipidx, pipadp in g_pipeline:
                write_file.write(dumps(pipadp, cls=CustomEncoder) + '\n')
    else:

        # check if user cleared the pipeline
        if FORM_PIP_CLR in request.args:
            g_pipeline.clear()
            init_list_of_global_graphs()

        # check if user submitted an adapter
        elif FORM_ADPTR_SUBMT in request.args:
            new_adapter = (get_next_index_in_g_pipeline(), deepcopy(adp))
            g_pipeline.append(new_adapter)

        # check if user updated any field in pipeline (or graph instance added)
        elif FORM_PIP_UPDT in request.args or \
            was_graph_instance_added(request.args):

            # iterate over adapter-cards
            for arg_name, arg_val in request.args.items():
                if ('inputs' in arg_name or 'outputs' in arg_name) and arg_val != '':

                    input_not_output = True
                    if 'outputs' in arg_name:
                        input_not_output = False

                    if GRAPH_INST_ADD == arg_val:
                        g_graphs_dropdown_list.append(GRAPH_INST_W_REPR + str(len(g_graphs_dropdown_list) - 1))
                        arg_val = g_graphs_dropdown_list[-1]

                    element_id_in_pipeline = int(arg_name.split('.')[0])
                    element_attr_in_pipeline = arg_name.split('.')[2]
                    update_g_pipeline_elements(element_id_in_pipeline, input_not_output, element_attr_in_pipeline, arg_val)

        # iterate over args and check is something should be removed
        for arg_name, arg_val in request.args.items():
            if FORM_ADPTR_RMV in arg_name:
                element_id_in_pipeline = int(arg_name.split('.')[0])
                remove_adapter_from_pipeline(element_id_in_pipeline)
                break

        # execute pipeline if user requested that
        if FORM_PIP_EXE in request.args:
            execute_pipeline()

    return render_template('pipeline.html', adp_dropdown_list=list_of_adapter_names, \
        adp_dropdown_selected_str=adp_id_str_chosen, adp_dropdown_selected_inst=adp, \
        pipeline_adapters=g_pipeline, pipeline_exe_msg=pip_exe_msg, graphs_dropdown_list=g_graphs_dropdown_list)

@app.route('/browse')
def browse():
    global g_adapterdb
    aptrs_cat = g_adapterdb.get_list_of_adapters()
    return render_template('browse.html', adapters_catalog=aptrs_cat)

# Set "homepage" to index.html
@app.route('/')
def index():
    return render_template('index.html')

# --- main --------------------------------------------------------------------

if __name__ == '__main__':
    global g_pipeline, g_adapterdb
    g_pipeline = list()
    g_adapterdb = AdapterDB()
    init_list_of_global_graphs()

    app.debug = True
    app.run(host='0.0.0.0', port=5000, debug=True)

from flask import Flask, render_template, request, session, send_file
from copy import deepcopy
from io import BytesIO
import funcs
from funcs import *
from dtran import Pipeline
from json import dumps, JSONEncoder, JSONDecoder
from os.path import join, splitext
from pathlib import Path
from datetime import datetime

UPLOAD_FOLDER = '/tmp/mint_dt/'

app = Flask(__name__)
app.secret_key = 'MFwwDQYJKoZIhvcNAQEBBQAD'
app.config['SESSION_TYPE'] = 'filesystem'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

KEY_DESC           = 'description'
KEY_MODL           = '__module__'
KEY_IDENTIFIER     = 'id'
KEY_INPUTS         = 'inputs'
KEY_OUTPUTS        = 'outputs'

FORM_ADPTR_SLCTD   = 'slct_adp'
FORM_ADPTR_SUBMT   = 'add_adp'
FORM_ADPTR_RMV     = 'remove_from_pipe'
FORM_PIP_UPDT      = 'update_pipe'
FORM_PIP_CLR       = 'clear_pipe'
FORM_PIP_EXE       = 'exe_pipe'

GRAPH_INST_W_REPR  = 'GraphInstanceWire'
GRAPH_INST_ADD     = 'Create graph instance'
GRAPH_INST_CHANGE  = '__select'
# TODO: support NumPy instances

# --- helpers -----------------------------------------------------------------

class CustomEncoder(JSONEncoder):
    ''' Custom encoder used serialize dictionaries and classes (i.e. AdapterElement). '''

    def default(self, o):
        ''' Default encoder. '''

        return {'__{}__'.format(o.__class__.__name__): o.__dict__}

def upload_file(file):
    ''' Upload user file to local machine. '''

    Path(app.config['UPLOAD_FOLDER']).mkdir(exist_ok=True, parents=True)
    if file.filename != '' and file:
        _, fname_ext = splitext(file.filename)
        fname_prefix = datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
        filename = str(fname_prefix + fname_ext)
        full_path_filename = join(app.config['UPLOAD_FOLDER'], filename)
        file.save(full_path_filename)
        return full_path_filename
    return ''

def AdapterElement_from_json(json_obj):
    ''' Custom decoder used to deserialize AdapterElement class. '''

    adp_el_str = '__AdapterElement__'
    if adp_el_str in json_obj:
        return AdapterElement.from_json(json_obj[adp_el_str])
    else:
        return json_obj

def update_list_of_session_graphs_from_args_dict(args_dict):
    ''' Update the list of (local) session graph instancs
    based on a given arguments dictionary.
    This method is called when loading a pipeline file externally. '''

    sesh_g_instancs = session.get('sesh_g_instancs', None)
    if not sesh_g_instancs:
        sesh_g_instancs = ['', GRAPH_INST_ADD]
    else:
        sesh_g_instancs = JSONDecoder().decode(session['sesh_g_instancs'])

    for _, item in args_dict.items():
        if item['id'] == 'graph' and item['val'] not in sesh_g_instancs:
            sesh_g_instancs.append(item['val'])
    session['sesh_g_instancs'] = dumps(sesh_g_instancs)

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

        name        = json_object['name']
        module_type = json_object['module_type']
        identifier  = json_object['identifier']
        description = json_object['description']
        inputs      = json_object['inputs']
        outputs     = json_object['outputs']

        # update local list of graph instances when loading from JSON file
        update_list_of_session_graphs_from_args_dict(inputs)
        update_list_of_session_graphs_from_args_dict(outputs)

        return cls(name, module_type, identifier, description, inputs, outputs)

    def __init__(self, name, module_type, identifier, description, inputs, outputs):
        ''' Initialize AdapterElement. '''

        self.name        = name
        self.module_type = module_type
        self.identifier  = identifier
        self.description = description
        self.inputs      = inputs
        self.outputs     = outputs
    
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
                cls_dict    = a_cls.__dict__
                module_type = cls_dict[KEY_MODL].split('.')[-1]
                identifier  = cls_dict[KEY_IDENTIFIER]
                description = None
                if KEY_DESC in cls_dict:
                    description = cls_dict[KEY_DESC]
                inputs_dict  = cls_dict[KEY_INPUTS]
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

def get_next_index_in_session_pipeline(session_pipeline):
    ''' Get the next available index in the (local) session pipeline. '''

    if len(session_pipeline) == 0:
        return 0
    else:
        return session_pipeline[-1][0] + 1

def remove_adapter_from_session_pipeline(session_pipeline, adapter_identifier_in_pipe):
    ''' Remove an adapter from the (local) session pipeline by index. '''

    for index_in_list, adapter_tuple in enumerate(session_pipeline):
        if adapter_identifier_in_pipe == adapter_tuple[0]:
            index_to_access = index_in_list
            break
    session_pipeline.pop(index_to_access)

def update_session_pipeline_fields(session_pipeline, adapter_identifier_in_pipe, input_n_output, adapter_attribute, value):
    ''' Update the (local) session pipeline adapter fields (content). '''

    ''' session_pipeline is a list
        ...[adapter_identifier_in_pipe] is an element in the pipe
        ...[0] holds the index, ...[1] holds the instance of the adapter (AdapterElement) '''

    # find the access index to the pipeline
    for index_in_list, adapter_tuple in enumerate(session_pipeline):
        if adapter_identifier_in_pipe == adapter_tuple[0]:
            index_to_access = index_in_list
            break
    # inputs/outputs hold a dictionary of attributes, each is a dictionary by itself
    if input_n_output:
        session_pipeline[index_to_access][1].inputs[adapter_attribute]['val'] = value
    else:
        session_pipeline[index_to_access][1].outputs[adapter_attribute]['val'] = value

def execute_session_pipeline(session_pipeline):
    ''' Execute the (local) session pipeline. '''

    global g_adapterdb

    inputs = {}
    pipeline_classes = []
    pipeline_wires = []

    gid = dict()
    '''
    'gid' (stands for graph instances dictionary)
    is a dictionary we construct in order to create
    a valid instance of the pipeline wiring.

    'graph_instance_name' -->
        'i/input'  --> list( (adp_identifier, None, adp_input_key ) )
        'o/output' --> list( (adp_identifier, None, adp_output_key) )
    '''

    ''' In order to invoke pipeline execution we need
    to construct the pipeline classes and the pipeline wiring. '''

    # iterate over the session pipeline instance
    for _, adapter in session_pipeline:
        # add adapter to list of pipeline classes
        adptr_name = adapter.get_adapter_name()
        adptr_obj  = g_adapterdb.get_adapter_object_from_name(adptr_name)
        adptr_id   = adapter.get_adapter_identifier()
        pipeline_classes.append(adptr_obj)

        # parse 'external' inputs and construct the wiring ('gid')
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
                    if adp_inp_val not in gid:
                        gid[adp_inp_val] = dict()
                    # 'i' stands for 'input'
                    if 'i' not in gid[adp_inp_val]:
                        gid[adp_inp_val]['i'] = list()
                    gid[adp_inp_val]['i'].append( [adptr_id, None, adp_inp_key] )

        # continue the wiring construction (iterate over outputs)
        for adp_out_key, adp_out_dict in adapter.outputs.items():
            adp_out_val = adp_out_dict['val']
            if adp_out_val != '' and adp_out_val != None and GRAPH_INST_W_REPR in adp_out_val:
                if adp_out_val not in gid:
                    gid[adp_out_val] = dict()
                # 'o' stands for 'output'
                if 'o' not in gid[adp_out_val]:
                    gid[adp_out_val]['o'] = list()
                gid[adp_out_val]['o'].append( [adptr_id, None, adp_out_key] )

    for _, graph_inst_list in gid.items():
        # only one output wire can exist!
        assert len(graph_inst_list['o']) == 1
        output_wire = graph_inst_list['o'][0]
        # the output wire can feed more than 1 input (input_wire)
        assert len(graph_inst_list['i']) >= 1
        for input_wire in graph_inst_list['i']:
            wire = ( input_wire, output_wire )
            pipeline_wires.append(wire)
    
    # set Pipeline Object
    pipeline = Pipeline(pipeline_classes, wired=pipeline_wires)

    # TODO: wrap with 'try' and print errors/results (return False)
    pipeline.exec(inputs)

    # TODO: create 'enums' for pipeline execution status
    return True

def download_configuration_file():
    ''' Save a pipeline configuration from web browser view. '''

    # init session pipeline
    sesh_pip = session.get('sesh_pip', None)
    if not sesh_pip:
        sesh_pip = list()
    else:
        sesh_pip = JSONDecoder(object_hook = AdapterElement_from_json).decode(session['sesh_pip'])

    tempfile = BytesIO()
    for _, pipadp in sesh_pip:
        tempfile.write((dumps(pipadp, cls=CustomEncoder) + '\n').encode())
    tempfile.seek(0)
    output_filename = datetime.now().strftime("pip_cfg__%Y_%m_%d__%H_%M_%S.json")
    return send_file(tempfile,
                     as_attachment=True,
                     attachment_filename=output_filename,
                     mimetype="application/json")


def load_configuration_file(file_path):
    ''' Load a pipeline configuation file into browser view. '''

    sesh_pip = list()
    session['sesh_g_instancs'] = dumps(['', GRAPH_INST_ADD])

    # load a pipeline configuration from file
    with open(file_path, 'r') as read_file:
        for line_i, line_d in enumerate(read_file):
            adapter_el = JSONDecoder(object_hook = AdapterElement_from_json).decode(line_d)
            load_adapter = (line_i, adapter_el)
            sesh_pip.append(load_adapter)
    session['sesh_pip'] = dumps(sesh_pip, cls=CustomEncoder)

# --- entrypoints -------------------------------------------------------------

@app.route('/pipeline', methods=['GET', 'POST'])
def pipeline():
    ''' Generates an interactive view of the pipeline elements (adapters)
    during its construction and execution.  '''

    global g_adapterdb
    pip_exe_msg = ('', '')

    # we call post only when we upload data
    if request.method == 'POST':
        # check if the post request has the file part
        if 'loadfile' in request.files and request.files['loadfile']:
            file = request.files['loadfile']
            full_path_fname = upload_file(file)
            if full_path_fname != '':
                load_configuration_file(full_path_fname, )
                pip_exe_msg = ('Uploaded file successfully!', 'success')
            else:
                pip_exe_msg = ('Failed to upload file...', 'danger')
        else:
            return download_configuration_file()


    # init wire 'instances' of graphs
    sesh_g_instancs = session.get('sesh_g_instancs', None)
    if not sesh_g_instancs:
        sesh_g_instancs = ['', GRAPH_INST_ADD]
    else:
        sesh_g_instancs = JSONDecoder().decode(session['sesh_g_instancs'])

    # init session pipeline
    sesh_pip = session.get('sesh_pip', None)
    if not sesh_pip:
        sesh_pip = list()
    else:
        sesh_pip = JSONDecoder(object_hook = AdapterElement_from_json).decode(session['sesh_pip'])

    # get list of adapters and their names
    list_of_adapters = g_adapterdb.get_list_of_adapters()
    list_of_adapter_names = g_adapterdb.get_list_of_adapter_names_for_dropdown()

    # set default option
    adp_id_str_chosen = list_of_adapter_names[0]
    # show selected adapter
    if FORM_ADPTR_SLCTD in request.args:
        adp_id_str_chosen = request.args.get(FORM_ADPTR_SLCTD)
    adp = list_of_adapters[int(adp_id_str_chosen.split(' ')[0])]

    if request.method != 'POST':
        # check if user cleared the pipeline
        if FORM_PIP_CLR in request.args:
            sesh_pip.clear()
            sesh_g_instancs = ['', GRAPH_INST_ADD]

        # check if user submitted an adapter
        elif FORM_ADPTR_SUBMT in request.args:
            new_adapter = (get_next_index_in_session_pipeline(sesh_pip), deepcopy(adp))
            sesh_pip.append(new_adapter)

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
                        sesh_g_instancs.append(GRAPH_INST_W_REPR + str(len(sesh_g_instancs) - 1))
                        arg_val = sesh_g_instancs[-1]

                    element_id_in_pipeline = int(arg_name.split('.')[0])
                    element_attr_in_pipeline = arg_name.split('.')[2]
                    update_session_pipeline_fields(sesh_pip, element_id_in_pipeline, input_not_output, element_attr_in_pipeline, arg_val)

        # iterate over args and check is something should be removed
        for arg_name, arg_val in request.args.items():
            if FORM_ADPTR_RMV in arg_name:
                element_id_in_pipeline = int(arg_name.split('.')[0])
                remove_adapter_from_session_pipeline(sesh_pip, element_id_in_pipeline)
                break

        # execute pipeline if user requested that
        if FORM_PIP_EXE in request.args:
            exec_sts = execute_session_pipeline(sesh_pip)
            if exec_sts:
                pip_exe_msg = ("Pipeline execution succeeded!", 'success')
            else:
                pip_exe_msg = ("Oops! Pipeline execution failed, see log...", 'danger')

    session['sesh_pip'] = dumps(sesh_pip, cls=CustomEncoder)
    session['sesh_g_instancs'] = dumps(sesh_g_instancs)

    return render_template('pipeline.html', adp_dropdown_list=list_of_adapter_names, \
        adp_dropdown_selected_str=adp_id_str_chosen, adp_dropdown_selected_inst=adp, \
        pipeline_adapters=sesh_pip, pipeline_exe_msg=pip_exe_msg, graphs_dropdown_list=sesh_g_instancs)

@app.route('/browse')
def browse():
    ''' Generates an view of all available pipeline elements (adapters). '''

    global g_adapterdb
    aptrs_cat = g_adapterdb.get_list_of_adapters()
    return render_template('browse.html', adapters_catalog=aptrs_cat)

# Set "homepage" to index.html
@app.route('/')
def index():
    ''' Generates the homepage. '''

    return render_template('index.html')

# --- main --------------------------------------------------------------------

if __name__ == '__main__':
    global g_adapterdb

    g_adapterdb = AdapterDB()
    app.debug = True
    app.run(host='0.0.0.0', port=5000, debug=True)

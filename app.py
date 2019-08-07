from flask import Flask, render_template, request
from copy import deepcopy
import funcs

app = Flask(__name__)

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'

FORM_ADAPTER_SLCTD = 'slct_adp'
FORM_ADAPTER_SUBMT = 'add_adp'
FORM_PIPELINE_UPDT = 'update_pipe'

class AdapterElement:
    def __init__(self, name, module_type, identifier, description, inputs, outputs):
        self.name = name
        self.module_type = module_type
        self.identifier = identifier
        self.description = description
        self.inputs = inputs
        self.outputs = outputs
    
    def __repr__(self):
        return f'name={self.name}, module_type={self.module_type}, ' + \
            f'identifier={self.identifier}\ndescription={self.description}\n' + \
            f'inputs={self.inputs}\noutputs={self.outputs}\n\n'

class AdapterDB:
    def __init__(self):
        self.adapters = list()
        self.initialize_adapters()

    def initialize_adapters(self):
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

    def get_list_of_adapters(self):
        return self.adapters

    def get_list_of_adapter_names_for_dropdown(self):
        adapter_names_list = list()
        for adp_idx, adp_ins in enumerate(self.get_list_of_adapters()):
            adapter_names_list.append(f'{adp_idx} {adp_ins.identifier}/{adp_ins.name}')
        return adapter_names_list

################################

def get_next_index_in_g_pipeline():
    global g_pipeline

    if len(g_pipeline) == 0:
        return 0
    else:
        return g_pipeline[-1][0] + 1

def update_g_pipeline_elements(adapter_index_in_pipe, input_n_output, adapter_attribute, value):
    global g_pipeline

    ''' g_pipeline is a list
        ...[adapter_index_in_pipe] is an element in the pipe
        ...[0] holds the index, ...[1] hold the instance of the adapter '''

    # inputs/outputs hold a dictionary of attributes, each one is a dictionary by itself '''
    if input_n_output:
        g_pipeline[adapter_index_in_pipe][1].inputs[adapter_attribute]['val'] = value
    else:
        g_pipeline[adapter_index_in_pipe][1].outputs[adapter_attribute]['val'] = value

@app.route('/pipeline', methods=['GET', 'POST'])
def pipeline():
    global g_pipeline, g_adapterdb

    # get list of adapters and their names
    list_of_adapters = g_adapterdb.get_list_of_adapters()
    list_of_adapter_names = g_adapterdb.get_list_of_adapter_names_for_dropdown()

    # set default option
    adp_id_str_chosen = list_of_adapter_names[0]
    # show selected adapter
    if FORM_ADAPTER_SLCTD in request.args:
        adp_id_str_chosen = request.args.get(FORM_ADAPTER_SLCTD)
    adp = list_of_adapters[int(adp_id_str_chosen.split(' ')[0])]

    # check if user submitted an adapter
    if FORM_ADAPTER_SUBMT in request.args:
        new_adapter = (get_next_index_in_g_pipeline(), deepcopy(adp))
        g_pipeline.append(new_adapter)

    # check if user updated any field in pipeline
    if FORM_PIPELINE_UPDT in request.args:
        for arg_name, arg_val in request.args.items():
            if ('inputs' in arg_name or 'outputs' in arg_name) and arg_val != '':
                input_not_output = True
                if 'outputs' in arg_name:
                    input_not_output = False
                element_id_in_pipeline = int(arg_name.split('.')[0])
                element_attr_in_pipeline = arg_name.split('.')[2]
                update_g_pipeline_elements(element_id_in_pipeline, input_not_output, element_attr_in_pipeline, arg_val)

    return render_template('pipeline.html', adp_dropdown_list=list_of_adapter_names, \
        adp_dropdown_selected_str=adp_id_str_chosen, adp_dropdown_selected_inst=adp, \
        pipeline_adapters=g_pipeline)

# Set "homepage" to index.html
@app.route('/')
def index():
    return render_template('index.html')

'''
@app.route('/search')
def search():
    if FORM_SEARCH_FIELD in request.args:
        srch_str = request.args.get(FORM_SEARCH_FIELD)
    list_of_adapters = filter_by_keyword_substring(.. srch_str ..)
    return render_template('index.html', list_of_adapters = list_of_adapters)
'''

if __name__ == '__main__':
    global g_pipeline, g_adapterdb
    g_pipeline = list()
    g_adapterdb = AdapterDB()

    app.debug = True
    app.run(host='0.0.0.0', port=5000, debug=True)

from flask import Flask, render_template, request
import funcs

app = Flask(__name__)

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'

FORM_ADAPTER_SLCTD = 'slct_adp'
FORM_ADAPTER_SUBMT = 'add_adp'

class AdapterElement:
    def __init__(self, name, module_type, identifier, description, inputs, outputs):
        self.name = name
        self.module_type = module_type
        self.identifier = identifier
        self.description = description
        self.inputs = inputs
        self.outputs = outputs

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
                    arg_attr_dict = arg_attr.__dict__
                    inputs[arg_name] = (arg_attr_dict['id'], arg_attr_dict['val'], arg_attr_dict['optional'])
                for arg_name, arg_attr in outputs_dict.items():
                    arg_attr_dict = arg_attr.__dict__
                    outputs[arg_name] = (arg_attr_dict['id'], arg_attr_dict['val'], arg_attr_dict['optional'])
                self.adapters.append(AdapterElement(a_name, module_type, identifier, description, inputs, outputs))

    def get_list_of_adapters(self):
        return self.adapters

    def get_list_of_adapter_names_for_dropdown(self):
        adapter_names_list = list()
        for adp_idx, adp_ins in enumerate(self.get_list_of_adapters()):
            adapter_names_list.append(f'{adp_idx} {adp_ins.identifier}/{adp_ins.name}')
        return adapter_names_list

################################

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
        g_pipeline.append(adp)
    #return render_template('pipeline.html', list_of_adapters = list_of_adapters)
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

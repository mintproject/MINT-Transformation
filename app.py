from flask import Flask, render_template, request
import funcs

app = Flask(__name__)

# FORM_SEARCH_FIELD = 'srch_str'

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'

FORM_CHOSEN_ADAPTER = 'slct_adp'
FORM_ADAPTER_SUBMT = 'add_adp'

class AdapterView:
    def __init__(self, name, module_type, identifier, description, inputs, outputs):
        self.name = name
        self.module_type = module_type
        self.identifier = identifier
        self.description = description
        self.inputs = inputs
        self.outputs = outputs

def get_list_of_adapters():
    adapters_list = list()
    for a_name, a_cls in funcs.__dict__.items():
        if isinstance(a_cls, type):
            cls_dict = a_cls.__dict__
            module_type = cls_dict[KEY_MODL].split('.')[-1]
            identifier = cls_dict[KEY_IDENTIFIER]
            description = None
            if KEY_DESC in cls_dict:
                description = cls_dict[KEY_DESC]
            inputs = cls_dict[KEY_INPUTS]
            outputs = cls_dict[KEY_OUTPUTS]
            '''
            inputs: Dict[str, ArgType] = {}
            outputs: Dict[str, ArgType] = {}
            '''
            adapters_list.append(AdapterView(a_name, module_type, identifier, description, inputs, outputs))
    return adapters_list

def get_list_of_adapter_names(list_of_adapters):
    adapter_names_list = list()
    for adp_idx, adp_ins in enumerate(list_of_adapters):
        adapter_names_list.append(f'{adp_idx} {adp_ins.module_type}/{adp_ins.identifier}.{adp_ins.name}')
    return adapter_names_list

################################

@app.route('/pipeline', methods=['GET', 'POST'])
def pipeline():

    global g_pipeline

    # get list of adapters and their names
    list_of_adapters = get_list_of_adapters()
    list_of_adapter_names = get_list_of_adapter_names(list_of_adapters)

    # set default option
    adp_id_str_chosen = list_of_adapter_names[0]
    # show selected adapter
    if FORM_CHOSEN_ADAPTER in request.args:
        adp_id_str_chosen = request.args.get(FORM_CHOSEN_ADAPTER)
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
    global g_pipeline
    g_pipeline = list()
    app.debug = True
    app.run(host='0.0.0.0', port=5000, debug=True)

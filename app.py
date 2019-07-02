from flask import Flask, render_template, request
import funcs

app = Flask(__name__)

# FORM_SEARCH_FIELD = 'srch_str'

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'

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
    for name, cls in funcs.__dict__.items():
        if isinstance(cls, type):
            cls_dict = cls.__dict__
            module_type = cls.__dict__[KEY_MODL].split('.')[-1]
            identifier = cls.__dict__[KEY_IDENTIFIER]
            description = None            
            if KEY_DESC in cls_dict:
                description = cls_dict[KEY_DESC]
            inputs = cls.__dict__[KEY_INPUTS]
            outputs = cls.__dict__[KEY_OUTPUTS]
            '''
            inputs: Dict[str, ArgType] = {}
            outputs: Dict[str, ArgType] = {}
            '''
            adapters_list.append(AdapterView(name, module_type, identifier, description, inputs, outputs))
    return adapters_list

################################

@app.route('/pipeline')
def pipeline():
    list_of_adapters = get_list_of_adapters()
    return render_template('pipeline.html', list_of_adapters = list_of_adapters)

# Set "homepage" to index.html
@app.route('/')
def index():
    list_of_adapters = get_list_of_adapters()
    return render_template('index.html', list_of_adapters = list_of_adapters)

'''
@app.route('/search')
def search():
    if FORM_SEARCH_FIELD in request.args:
        srch_str = request.args.get(FORM_SEARCH_FIELD)
    list_of_adapters = filter_by_keyword_substring(.. srch_str ..)
    return render_template('index.html', list_of_adapters = list_of_adapters)
'''

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000, debug=True)

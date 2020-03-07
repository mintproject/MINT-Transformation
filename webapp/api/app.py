from flask import Flask, render_template
from flask_cors import CORS

from webapp.api.controllers.adapters import adapters_blueprint
from webapp.api.controllers.pipelines import pipelines_blueprint
import yaml
from collections import OrderedDict


def represent_dictionary_order(self, dict_data):
    return self.represent_mapping('tag:yaml.org,2002:map', dict_data.items())


def setup_yaml():
    yaml.add_representer(OrderedDict, represent_dictionary_order)


setup_yaml()

app = Flask(__name__)
CORS(app)

app.register_blueprint(adapters_blueprint, url_prefix='/api')
app.register_blueprint(pipelines_blueprint, url_prefix='/api')


@app.route("/", methods=["GET"])
@app.route("/<path:dyn_path>", methods=["GET"])
def index(dyn_path=None):
    return render_template("index.html")


if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=10010, debug=True)

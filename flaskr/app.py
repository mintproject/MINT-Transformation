from flask import Flask, render_template
from flask_cors import CORS

from flaskr.api.adapters import adapters_blueprint
from flaskr.api.pipelines import pipelines_blueprint
import os

app = Flask(__name__, static_url_path="", static_folder=os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"))
print(app.static_folder)
CORS(app)

app.register_blueprint(adapters_blueprint)
app.register_blueprint(pipelines_blueprint)


@app.route("/", methods=["GET"])
@app.route("/<path:dyn_path>", methods=["GET"])
def index(dyn_path=None):
    return render_template("index.html")


if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=10010, debug=True)

from flask import Flask
from flask_cors import CORS

from flaskr.api.adapters import adapters_blueprint
from flaskr.api.pipelines import pipelines_blueprint

app = Flask(__name__)
CORS(app)

app.register_blueprint(adapters_blueprint)
app.register_blueprint(pipelines_blueprint)


if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0', port=5000, debug=True)

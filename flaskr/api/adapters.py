from flask import Blueprint, jsonify
import json

KEY_DESC = 'description'
KEY_MODL = '__module__'
KEY_IDENTIFIER = 'id'
KEY_INPUTS = 'inputs'
KEY_OUTPUTS = 'outputs'


adapters_blueprint = Blueprint("adapters", "adapters", url_prefix="/api")


@adapters_blueprint.route('/adapters', methods=["GET"])
def list_adapters():
    # TODO: put adapters in db?
    with open("./adapters.json", "r") as f:
        adapters = json.load(f)
        parsed_adapters = [{
            "id": ad["name"],
            "description": ad["description"],
            "inputs": ad["input"],
            "outputs": ad["output"]
        } for ad in adapters]
    print("I'm returning")
    print(parsed_adapters[0])
    return jsonify(parsed_adapters)

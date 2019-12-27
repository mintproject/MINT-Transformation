from flask import Blueprint, jsonify

FAKE_ADAPTERS = [
    {
        "name": "Read Function",
        "func_name": "read_func",
        "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
        "input": {
            "repr_file": "[file_path] *",
            "resources": "[string] *"
        },
        "ouput": {
            "data": "[graph] *"
        }
    }, {
        "name": "Read Function",
        "func_name": "read_func",
        "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
        "input": {
            "repr_file": "[file_path] *",
            "resources": "[string] *"
        },
        "ouput": {
            "data": "[graph] *"
        }
    }, {
        "name": "Read Function",
        "func_name": "read_func",
        "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
        "input": {
            "repr_file": "[file_path] *",
            "resources": "[string] *"
        },
        "ouput": {
            "data": "[graph] *"
        }
    }, {
        "name": "Read Function",
        "func_name": "read_func",
        "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
        "input": {
            "repr_file": "[file_path] *",
            "resources": "[string] *"
        },
        "ouput": {
            "data": "[graph] *"
        }
    }, {
        "name": "Read Function",
        "func_name": "read_func",
        "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
        "input": {
            "repr_file": "[file_path] *",
            "resources": "[string] *"
        },
        "ouput": {
            "data": "[graph] *"
        }
    }, {
        "name": "Read Function",
        "func_name": "read_func",
        "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
        "input": {
            "repr_file": "[file_path] *",
            "resources": "[string] *"
        },
        "ouput": {
            "data": "[graph] *"
        }
    }, {
        "name": "Read Function",
        "func_name": "read_func",
        "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
        "input": {
            "repr_file": "[file_path] *",
            "resources": "[string] *"
        },
        "ouput": {
            "data": "[graph] *"
        }
    }
]

adapters_blueprint = Blueprint("adapters", "adapters", url_prefix="/api")


@adapters_blueprint.route('/adapters', methods=["GET"])
def list_adapters():
    # TODO: list real adapters: db?
    return jsonify(FAKE_ADAPTERS)

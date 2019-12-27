from flask import Blueprint, jsonify, request
import json
import yaml

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
FAKE_PIPELINES = [
    {
        "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
        "description": "blahblahblah",
        "start_timestamp": "2019-12-17T12:05:00",
        "status": "running",
        "end_timestamp": "",
        "config_file": "some yaml. decide later",
        "id": "123451",
        "adapters": FAKE_ADAPTERS
    }, {
        "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
        "description": "blahblahblah",
        "start_timestamp": "2019-12-16T12:05:00",
        "status": "finished",
        "end_timestamp": "2019-12-17T13:05:00",
        "config_file": "some yaml. decide later",
        "id": "123452",
        "adapters": FAKE_ADAPTERS
    }, {
        "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
        "description": "blahblahblah",
        "start_timestamp": "2019-12-15T12:05:00",
        "status": "running",
        "end_timestamp": "",
        "config_file": "some yaml. decide later",
        "id": "123453",
        "adapters": FAKE_ADAPTERS
    }, {
        "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
        "description": "blahblahblah",
        "start_timestamp": "2019-12-14T12:05:00",
        "status": "failed",
        "end_timestamp": "2019-12-14T17:05:00",
        "config_file": "some yaml. decide later",
        "id": "123454",
        "adapters": FAKE_ADAPTERS
    }, {
        "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
        "description": "blahblahblah",
        "start_timestamp": "2019-12-13T12:05:00",
        "status": "failed",
        "end_timestamp": "2019-12-14T17:05:00",
        "config_file": "some yaml. decide later",
        "id": "123455",
        "adapters": FAKE_ADAPTERS
    }, {
        "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
        "description": "blahblahblah",
        "start_timestamp": "2019-12-12T12:05:00",
        "status": "running",
        "end_timestamp": "",
        "config_file": "some yaml. decide later",
        "id": "123456",
        "adapters": FAKE_ADAPTERS
    }, {
        "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
        "description": "blahblahblah",
        "start_timestamp": "2019-12-11T12:05:00",
        "status": "failed",
        "end_timestamp": "2019-12-14T17:05:00",
        "config_file": "some yaml. decide later",
        "id": "123457",
        "adapters": FAKE_ADAPTERS
    }
]

pipelines_blueprint = Blueprint("pipelines", "pipelines", url_prefix="/api")

@pipelines_blueprint.route('/pipelines', methods=["GET"])
def list_pipelines():
    # TODO: add search parameters, list all entries from 'docker ps -a'
    return jsonify(FAKE_PIPELINES)


@pipelines_blueprint.route('/pipelines/<pipeline_id>', methods=["GET"])
def list_pipeline(pipeline_id):
    # TODO: this is fake endpoint
    print(f"want to find id: {pipeline_id}")
    return jsonify([x for x in FAKE_PIPELINES if x["id"] == pipeline_id][0])


@pipelines_blueprint.route('/pipeline/create', methods=["POST"])
def create_pipeline():
    # TODO: add docker run command
    return jsonify({
        "result": "success"
    })


@pipelines_blueprint.route('/pipeline/upload_config', methods=["POST"])
def upload_pipeline_config():
    # TODO: sub with real config yml
    if 'files' not in request.files:
        print("NOTHING IS UPLOADED!")
    else:
        # See documentation about file storage: https://werkzeug.palletsprojects.com/en/0.16.x/datastructures/#werkzeug.datastructures.FileStorage
        uploaded_file = request.files['files']
        print(uploaded_file)
        if 'json' in uploaded_file.filename:
            print("THIS IS A JSON FILE")
            json_data = json.load(uploaded_file)
            # print(json.dumps(json_data, indent=4))
            # TODO: parse the config and return the graph structure
            response = jsonify({
                "data": FAKE_PIPELINES[0]
            })
        elif 'yml' in uploaded_file.filename:
            print("THIS IS A YML FILE")
            yml_data = yaml.safe_load(uploaded_file)
            # print(json.dumps(yml_data, indent=4))
            response = jsonify({
                "data": FAKE_PIPELINES[2]
            })
        else:
            response = jsonify({
                "error": "Please upload json/yml config file!"
            })
    response.headers.add('Access-Control-Allow-Origin', '*')
    # TODO: should handle error messages here!
    # return parsed config to display in front end: validate/invalid reasons
    # dcat jump url: look at the existing example :)
    return response


@pipelines_blueprint.route('/pipeline/dcat/<dcat_id>', methods=["GET"])
def get_dcat_config(dcat_id):
    # TODO: connect with data catalog
    print(f"Fetching dcat dataset with id {dcat_id}")
    return jsonify(FAKE_PIPELINES[3])
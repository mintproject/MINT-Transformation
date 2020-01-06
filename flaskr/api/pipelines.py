from flask import Blueprint, jsonify, request
import json
import yaml
from dtran.config_parser import ConfigParser

from typing import *
from uuid import uuid4
import subprocess
from dataclasses import dataclass, asdict
import ujson
from datetime import datetime

@dataclass
class Pipeline:
    id: str
    name: str
    description: str
    start_timestamp: Optional[str]
    end_timestamp: Optional[str]
    # this config variable depends on what you will do with the pipeline configuration
    # may be dictionary?
    config: Any
    output: str
    status: str
# FAKE_ADAPTERS = [
#     {
#         "name": "Read Function",
#         "func_name": "read_func",
#         "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
#         "input": {
#             "repr_file": "[file_path] *",
#             "resources": "[string] *"
#         },
#         "ouput": {
#             "data": "[graph] *"
#         }
#     }, {
#         "name": "Read Function",
#         "func_name": "read_func",
#         "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
#         "input": {
#             "repr_file": "[file_path] *",
#             "resources": "[string] *"
#         },
#         "ouput": {
#             "data": "[graph] *"
#         }
#     }, {
#         "name": "Read Function",
#         "func_name": "read_func",
#         "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
#         "input": {
#             "repr_file": "[file_path] *",
#             "resources": "[string] *"
#         },
#         "ouput": {
#             "data": "[graph] *"
#         }
#     }, {
#         "name": "Read Function",
#         "func_name": "read_func",
#         "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
#         "input": {
#             "repr_file": "[file_path] *",
#             "resources": "[string] *"
#         },
#         "ouput": {
#             "data": "[graph] *"
#         }
#     }, {
#         "name": "Read Function",
#         "func_name": "read_func",
#         "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
#         "input": {
#             "repr_file": "[file_path] *",
#             "resources": "[string] *"
#         },
#         "ouput": {
#             "data": "[graph] *"
#         }
#     }, {
#         "name": "Read Function",
#         "func_name": "read_func",
#         "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
#         "input": {
#             "repr_file": "[file_path] *",
#             "resources": "[string] *"
#         },
#         "ouput": {
#             "data": "[graph] *"
#         }
#     }, {
#         "name": "Read Function",
#         "func_name": "read_func",
#         "description": "An entry point in the pipeline. Reads an input file and a yml file describing the D-REPR layout of this file. The data are representated in a Graph object.",
#         "input": {
#             "repr_file": "[file_path] *",
#             "resources": "[string] *"
#         },
#         "ouput": {
#             "data": "[graph] *"
#         }
#     }
# ]
# FAKE_PIPELINES = [
#     {
#         "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
#         "description": "blahblahblah",
#         "start_timestamp": "2019-12-17T12:05:00",
#         "status": "running",
#         "end_timestamp": "",
#         "config_file": "some yaml. decide later",
#         "id": "123451",
#         "adapters": FAKE_ADAPTERS
#     }, {
#         "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
#         "description": "blahblahblah",
#         "start_timestamp": "2019-12-16T12:05:00",
#         "status": "finished",
#         "end_timestamp": "2019-12-17T13:05:00",
#         "config_file": "some yaml. decide later",
#         "id": "123452",
#         "adapters": FAKE_ADAPTERS
#     }, {
#         "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
#         "description": "blahblahblah",
#         "start_timestamp": "2019-12-15T12:05:00",
#         "status": "running",
#         "end_timestamp": "",
#         "config_file": "some yaml. decide later",
#         "id": "123453",
#         "adapters": FAKE_ADAPTERS
#     }, {
#         "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
#         "description": "blahblahblah",
#         "start_timestamp": "2019-12-14T12:05:00",
#         "status": "failed",
#         "end_timestamp": "2019-12-14T17:05:00",
#         "config_file": "some yaml. decide later",
#         "id": "123454",
#         "adapters": FAKE_ADAPTERS
#     }, {
#         "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
#         "description": "blahblahblah",
#         "start_timestamp": "2019-12-13T12:05:00",
#         "status": "failed",
#         "end_timestamp": "2019-12-14T17:05:00",
#         "config_file": "some yaml. decide later",
#         "id": "123455",
#         "adapters": FAKE_ADAPTERS
#     }, {
#         "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
#         "description": "blahblahblah",
#         "start_timestamp": "2019-12-12T12:05:00",
#         "status": "running",
#         "end_timestamp": "",
#         "config_file": "some yaml. decide later",
#         "id": "123456",
#         "adapters": FAKE_ADAPTERS
#     }, {
#         "name": "Some pipeline running on ALWERO GLDAS data 2008 - 2018",
#         "description": "blahblahblah",
#         "start_timestamp": "2019-12-11T12:05:00",
#         "status": "failed",
#         "end_timestamp": "2019-12-14T17:05:00",
#         "config_file": "some yaml. decide later",
#         "id": "123457",
#         "adapters": FAKE_ADAPTERS
#     }
# ]


pipelines_blueprint = Blueprint("pipelines", "pipelines", url_prefix="/api")


@pipelines_blueprint.route('/pipelines', methods=["GET"])
def list_pipelines():
    # TODO: add search parameters, list all entries from 'docker ps -a'
    try:
        pipelines = list_pipelines_detail()
        return jsonify(pipelines)
    except Exception as e:
        return jsonify({"error": str(e)})


@pipelines_blueprint.route('/pipelines/<pipeline_id>', methods=["GET"])
def list_pipeline(pipeline_id):
    try:
        pipeline = list_pipeline_detail(pipeline_id)
        return jsonify(pipeline)
    except Exception as e:
        return jsonify({"error": str(e)})


@pipelines_blueprint.route('/pipeline/create', methods=["POST"])
def create_pipeline():
    pipeline_name = request.json.get("name", "")
    pipeline_description = request.json.get("description", "")
    pipeline_config = request.json.get("config", "")
    try:
        run_pipeline(pipeline_name, pipeline_description, pipeline_config)
        return jsonify({
            "result": "success"
        })
    except Exception as e:
        return jsonify({"error": str(e)})


@pipelines_blueprint.route('/pipeline/upload_config', methods=["POST"])
def upload_pipeline_config():
    if 'files' not in request.files:
        print("NOTHING IS UPLOADED!")
    else:
        # See documentation about file storage: https://werkzeug.palletsprojects.com/en/0.16.x/datastructures/#werkzeug.datastructures.FileStorage
        uploaded_file = request.files['files']
        if 'json' in uploaded_file.filename:
            config = json.load(uploaded_file)
        elif 'yml' in uploaded_file.filename:
            config = yaml.safe_load(uploaded_file)
        else:
            return jsonify({
                "error": "Please upload json/yml config file!"
            })
    # TODO: should handle error messages here!
    try:
        parser = ConfigParser({})
        parsed_pipeline, parsed_inputs = parser.parse(conf_obj=config)
        display_data = {
            "funcs": [{
                "id": func.id,
                "description": func.description,
                "inputs": None,
                "outputs": None
            } for func in parsed_pipeline.func_classes],
            "inputs": parsed_inputs
        }
        return jsonify({"data": display_data, "config": config})
    except Exception as e:
        return jsonify({"error": str(e)})


@pipelines_blueprint.route('/pipeline/dcat/<dcat_id>', methods=["GET"])
def get_dcat_config(dcat_id):
    # TODO: connect with data catalog
    try:
        print(f"Fetching dcat dataset with id {dcat_id}")
        return jsonify({
            "error": "WIP"
        })
    except Exception as e:
        return jsonify({"error": str(e)})

# ------ fake pipeline -------


def is_valid_id(id: str) -> bool:
    # TODO: validate the id to make sure that users don't inject any malicious commands in here
    # perhaps just need to make sure that only contains [a-z0-9] and `-` characters.
    if "-" in id:
        id = id.replace("-", "")
    return id.isalnum()


def run_pipeline(name: str, description: str, config: object, id=""):
    if id == "":
        id =str(uuid4())
    if not is_valid_id(id):
        return jsonify({
            "error": "invalid pipeline id"
        })

    sess_id = str(uuid4())
    host_conf_file = f"/tmp/config.{sess_id}.yml"
    host_start_log_file = f"/tmp/run.start.{sess_id}.log"
    host_end_log_file = f"/tmp/run.end.{sess_id}.log"

    with open(host_conf_file, "w") as f:
        yaml.safe_dump(config, f)

    with open(host_start_log_file, "w") as f, open(host_end_log_file, "w"):
        # TODO: write details of pipeline
        start_time = datetime.now()
        ujson.dump({
            "name": name,
            "description": description,
            "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
            # "config": config
        }, f)

    # TODO: modify the command to mount directory or add environment variables if needed
    subprocess.check_call(
        f"docker run --name {id} -d -v /tmp:/tmp -v $(pwd):/ws mint_dt bash /ws/scripts/run_pipeline_from_ui.sh {sess_id}", shell=True
    )


def list_pipeline_detail(id: str):
    if not is_valid_id(id):
        return jsonify({
            "error": "invalid pipeline id to display"
        })

    sess_id = str(uuid4())
    host_conf_file = f"/tmp/config.{sess_id}.yml"
    host_start_log_file = f"/tmp/run.start.{sess_id}.log"
    host_end_log_file = f"/tmp/run.end.{sess_id}.log"

    container_conf_file = "/run/config.yml"  # the file we are going to write the config to
    container_start_log_file = "/run/start.run.log"  # the file that keeps the start_time
    container_end_log_file = "/run/end.run.log"  # the file that keeps the end_time

    subprocess.check_call(f"docker cp {id}:{container_conf_file} {host_conf_file} && docker cp {id}:{container_start_log_file} {host_start_log_file} && docker cp {id}:{container_end_log_file} {host_end_log_file}", shell=True)
    run_log = subprocess.check_output(f"docker logs {id}", shell=True)
    # TODO: deserialize the configuration file and the log file to get the pipeline and return it.
    with open(host_start_log_file, "r") as f1, open(host_end_log_file, "r") as f2, open(host_conf_file, "r") as f3:
        start_log = json.load(f1)
        end_log = f2.readline()
        config = yaml.safe_load(f3)
        if end_log:
            status = "finished"
        else:
            status = "running"
        pipeline = Pipeline(
            id=id,
            name=start_log.get("name", ""),
            description=start_log.get("description", ""),
            start_timestamp=start_log.get("start_time", ""),
            end_timestamp=end_log,
            config=config,
            output=run_log.decode("utf-8"),
            status=status
        )
        return asdict(pipeline)


def list_pipelines_detail():
    # TODO: parse the JSON output and return a list of pipelines, some information that is expensive to obtain such as
    #  output, configuration file can be omitted. Use pagination to save query
    # TODO: THIS IS WAY TOO SLOW
    pipeline_ids = list_all_pipelines_docker()
    return [list_pipeline_detail(pid) for pid in pipeline_ids]


def list_all_pipelines_docker():
    output = subprocess.check_output("docker ps -a --format='{{json .ID}}'", shell=True)
    pipeline_ids_quoted = str.splitlines(output.decode("utf-8"))
    return [pid.replace('"', '') for pid in pipeline_ids_quoted]
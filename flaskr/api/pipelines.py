from flask import Blueprint, jsonify, request
import json
import yaml
from dtran.config_parser import ConfigParser
from dcatreg.dcat_api import DCatAPI

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


DCATAPI_INSTANCE = DCatAPI()


pipelines_blueprint = Blueprint("pipelines", "pipelines", url_prefix="/api")


@pipelines_blueprint.route('/test', methods=["GET"])
def test_func():
    # Get adapter in dict format
    # from funcs import ReadFunc
    # data = ReadFunc("./wfp_food_prices_south-sudan.repr.yml", "./wfp_food_prices_south-sudan.csv")
    # data_str = data.to_dict()
    # # data_str = data.adapter2dict()
    # print(data_str)
    test_parser = ConfigParser({})
    parsed_pipeline, parsed_inputs = test_parser.parse(path="./test_long.yml")
    # import pdb; pdb.set_trace()
    data_str = parsed_pipeline.graph_inputs_to_json(parsed_inputs)
    return jsonify({ "res": data_str })


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
    pipeline_name = request.json.get("name", "Unnamed")
    pipeline_description = request.json.get("description", "No Description")
    pipeline_nodes = request.json.get("nodes", "")
    pipeline_edges = request.json.get("edges", "")
    try:
        # TODO: de-serialize the pipeline here
        # run_pipeline(pipeline_name, pipeline_description, pipeline_config)
        print(json.dumps(pipeline_nodes, indent=2))
        print(json.dumps(pipeline_edges, indent=2))
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
        display_data = parsed_pipeline.graph_inputs_to_json(parsed_inputs)
        return jsonify({"data": display_data})
    except Exception as e:
        return jsonify({"error": str(e)})


@pipelines_blueprint.route('/pipeline/dcat/<dcat_id>', methods=["GET"])
def get_dcat_config(dcat_id: str):
    # TODO: connect with data catalog
    try:
        dataset = DCATAPI_INSTANCE.find_dataset_by_id(dcat_id)
        dataset_config = dataset["dataset_metadata"].get("config", None)
        if dataset_config is None:
            return jsonify({
                "error": "This dataset has no config associated!"
            })
        else:
            parser = ConfigParser({})
            parsed_pipeline, parsed_inputs = parser.parse(conf_obj=dataset_config)
            display_data = parsed_pipeline.graph_inputs_to_json(parsed_inputs)
            return jsonify({"data": display_data})
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
        end_log = f2.read().splitlines()[0]
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
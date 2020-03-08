# from flask import Blueprint, jsonify, request
# import json
# import yaml
# from dcatreg.dcat_api import DCatAPI
# from webapp.flaskr.config_graph_parser import DiGraphSchema
import glob
import json
import os
import subprocess
from pathlib import Path
from collections import OrderedDict
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import *
from uuid import uuid4

import ujson
import yaml
from funcs.readers.dcat_read_func import DCatAPI
from flask import Blueprint, jsonify, request
from api.config_graph_parser import DiGraphSchema

TMP_DIR = "/tmp/mintdt"
PROJECT_DIR = str(Path(os.path.abspath(__file__)).parent.parent.parent.parent)


def setup_mintdt():
    if not os.path.isdir(TMP_DIR):
        print(f"Creating {TMP_DIR}...")
        os.mkdir(f"{TMP_DIR}")
    else:
        print(f"{TMP_DIR} already exists...")


def setup_yaml():
    """ https://stackoverflow.com/a/8661021 """
    represent_dict_order = lambda self, data: self.represent_mapping('tag:yaml.org,2002:map',
                                                                     data.items())
    yaml.add_representer(OrderedDict, represent_dict_order)


setup_yaml()
setup_mintdt()


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


DCATAPI_INSTANCE = DCatAPI.get_instance()

pipelines_blueprint = Blueprint("pipelines", "pipelines", url_prefix="/api")


@pipelines_blueprint.route('/test', methods=["GET"])
def test_func():
    # Get adapter in dict format
    # from funcs import ReadFunc
    # data = ReadFunc("./wfp_food_prices_south-sudan.repr.yml", "./wfp_food_prices_south-sudan.csv")
    # data_str = data.to_dict()
    # # data_str = data.adapter2dict()
    # print(data_str)
    with open("./test_long.yml", "r") as f:
        config = yaml.safe_load(f)
    print(config)
    # import pdb; pdb.set_trace()
    display_data = DiGraphSchema().dump(config)
    return jsonify({"data": display_data})


@pipelines_blueprint.route('/pipelines', methods=["GET"])
def list_pipelines():
    # TODO: add search parameters, list all entries from 'docker ps -a'
    try:
        pipelines = list_pipelines_detail()
        return jsonify(pipelines), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pipelines_blueprint.route('/pipelines/<pipeline_id>', methods=["GET"])
def list_pipeline(pipeline_id):
    try:
        pipeline_ids, pipeline_names = list_all_pipelines_docker()
        for pid, pn in zip(pipeline_ids, pipeline_names):
            if pipeline_id == pn:
                pipeline = list_pipeline_detail(pid, pn)
                return jsonify(pipeline), 200
        return jsonify({"error": "No such pipeline exists!"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@pipelines_blueprint.route('/pipeline/create', methods=["POST"])
def create_pipeline():
    pipeline_name = request.json.get("name", "")
    pipeline_description = request.json.get("description", "")
    if pipeline_description == "":
        pipeline_description = "No Description"
    if pipeline_name == "":
        pipeline_name = str(uuid4())
    pipeline_nodes = request.json.get("nodes", [])
    pipeline_edges = request.json.get("edges", [])
    try:
        # TODO: de-serialize the pipeline here and get config
        pipeline_config = DiGraphSchema().load({
            "version": "1",
            "description": pipeline_description,
            "nodes": pipeline_nodes,
            "edges": pipeline_edges
            # TODO: Missing comment
        })
        # import pdb; pdb.set_trace()
        print(json.dumps(pipeline_config, indent=2))
        # run_pipeline(pipeline_name, pipeline_description, dict(pipeline_config))
        # print(json.dumps(pipeline_nodes, indent=2))
        # print(json.dumps(pipeline_edges, indent=2))
        return jsonify({"result": "success"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


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
            return jsonify({"error": "Please upload json/yml config file!"}), 400
    # TODO: should handle error messages here!
    try:
        display_data = DiGraphSchema().dump(config)
        return jsonify({"data": display_data}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400


# @pipelines_blueprint.route('/pipeline/dcat/<dcat_id>', methods=["GET"])
# def get_dcat_config(dcat_id: str):
#     # TODO: connect with data catalog
#     try:
#         dataset = DCATAPI_INSTANCE.find_dataset_by_id(dcat_id)
#         dataset_config = dataset["dataset_metadata"].get("config", None)
#         if dataset_config is None:
#             return jsonify({
#                 "error": "This dataset has no config associated!"
#             }), 400
#         else:
#             print(dataset_config)
#             display_data = DiGraphSchema().dump(dataset_config)
#             print(display_data)
#             return jsonify({"data": display_data}), 200
#     except Exception as e:
#         print(e)
#         return jsonify({"error": str(e)}), 400

# ------ fake pipeline -------


def is_valid_id(id: str) -> bool:
    # TODO: validate the id to make sure that users don't inject any malicious commands in here
    # perhaps just need to make sure that only contains [a-z0-9] and `-` characters.
    if "-" in id:
        id = id.replace("-", "")
    return id.isalnum()


def run_pipeline(name: str, description: str, config: object, id=""):
    if id == "":
        id = str(uuid4())
    if not is_valid_id(id):
        return jsonify({"error": "invalid pipeline id"})

    # print(f"creating pipeline id: {id}")

    sess_id = str(uuid4())
    host_conf_file = f"/tmp/config.{sess_id}.yml"
    host_start_log_file = f"/tmp/run.start.{sess_id}.log"
    host_end_log_file = f"/tmp/run.end.{sess_id}.log"
    host_cache = f"{TMP_DIR}/{id}.json"

    with open(host_conf_file, "w") as f:
        yaml.dump(config, f)

    with open(host_start_log_file, "w") as f, open(host_end_log_file, "w"), open(host_cache,
                                                                                 "w") as fd:
        # TODO: write details of pipeline
        start_time = datetime.now()
        ujson.dump(
            {
                "name": name,
                "description": description,
                "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                # "config": config
            },
            f)
        # print(f"Writing to {TMP_DIR}/{id}.json")
        ujson.dump(
            {
                "config": config,
                "start": {
                    "name": name,
                    "description": description,
                    "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                },
                "end": {}
            }, fd)

    print("---------------------------------------------")
    # TODO: modify the command to mount directory or add environment variables if needed
    command = f"docker run --name {id} " + \
                "-v /tmp:/tmp " + \
                "-v $(pwd):/ws " + \
                "-w /ws -d mint_dt " + \
                f"bash /ws/webapp/scripts/run_pipeline_from_ui.sh {sess_id}"

    print(command)
    subprocess.check_call(command, cwd=PROJECT_DIR, shell=True)
    return


def list_pipeline_detail(pid: str, id: str):
    if not is_valid_id(id):
        return jsonify({"error": "invalid pipeline id to display"})

    # check if it's already in docker id
    docker_names, filenames = list_all_files_mintdt()
    # print(f"current docker ids are: {docker_names}")
    if id in docker_names:
        # print(f"{TMP_DIR}/{id}.json exists!")
        with open(f"{TMP_DIR}/{id}.json", "r") as f:
            try:
                data = json.load(f)
            except ValueError:
                data = {}
            if "end" in data and data["end"]:
                # if the process has already finished
                # print(f"Reading from {TMP_DIR}/{id}.json")
                run_log = subprocess.check_output(f"docker logs {pid}", shell=True)
                # print("AM I HERE?")
                pipeline = Pipeline(id=id,
                                    name=data["start"].get("name", ""),
                                    description=data["start"].get("description", ""),
                                    start_timestamp=data["start"].get("start_time", ""),
                                    end_timestamp=data["end"],
                                    config=data["config"],
                                    output=run_log.decode("utf-8"),
                                    status="finished")
                return asdict(pipeline)

    # pipeline is still running: remove this json file
    if os.path.exists(f"{TMP_DIR}/{id}.json"):
        # print(f"Removing {TMP_DIR}/{id}.json since process is not finished")
        os.remove(f"{TMP_DIR}/{id}.json")

    sess_id = str(uuid4())
    host_conf_file = f"/tmp/config.{sess_id}.yml"
    host_start_log_file = f"/tmp/run.start.{sess_id}.log"
    host_end_log_file = f"/tmp/run.end.{sess_id}.log"
    host_cache = f"{TMP_DIR}/{id}.json"

    container_conf_file = "/run/config.yml"  # the file we are going to write the config to
    container_start_log_file = "/run/start.run.log"  # the file that keeps the start_time
    container_end_log_file = "/run/end.run.log"  # the file that keeps the end_time

    subprocess.check_call(
        f"docker cp {id}:{container_conf_file} {host_conf_file} && docker cp {id}:{container_start_log_file} {host_start_log_file} && docker cp {id}:{container_end_log_file} {host_end_log_file}",
        shell=True)
    run_log = subprocess.check_output(f"docker logs {pid}", shell=True)
    # TODO: deserialize the configuration file and the log file to get the pipeline and return it.
    with open(host_start_log_file,
              "r") as f1, open(host_end_log_file,
                               "r") as f2, open(host_conf_file, "r") as f3, open(host_cache,
                                                                                 "w") as fd:
        start_log = json.load(f1)
        config = yaml.safe_load(f3)
        # import pdb; pdb.set_trace()
        end_log = f2.read().splitlines()
        if len(end_log) > 0:
            print(f2.read().splitlines())
            data = {"config": config, "start": start_log, "end": end_log[0]}
            print(f"Writing to {TMP_DIR}/{id}.json (again)")
            status = "finished"
            ujson.dump(data, fd)
            end_log = end_log[0]
        else:
            ujson.dump({
                "config": config,
                "start": start_log,
            }, fd)
            status = "running"
            end_log = ""
        pipeline = Pipeline(id=id,
                            name=start_log.get("name", ""),
                            description=start_log.get("description", ""),
                            start_timestamp=start_log.get("start_time", ""),
                            end_timestamp=end_log,
                            config=config,
                            output=run_log.decode("utf-8"),
                            status=status)
        return asdict(pipeline)


def list_pipelines_detail():
    # TODO: parse the JSON output and return a list of pipelines, some information that is expensive to obtain such as
    #  output, configuration file can be omitted. Use pagination to save query
    # TODO: THIS IS WAY TOO SLOW
    pipeline_ids, pipeline_names = list_all_pipelines_docker()
    return [list_pipeline_detail(pid, pn) for pid, pn in zip(pipeline_ids, pipeline_names)]


def list_all_pipelines_docker():
    output = subprocess.check_output(
        "docker ps -a --format '{{json .ID}}' --filter ancestor=mint_dt", shell=True)
    pipeline_ids_quoted = str.splitlines(output.decode("utf-8"))
    output = subprocess.check_output("docker ps -a --format '{{.Names}}' --filter ancestor=mint_dt",
                                     shell=True)
    pipeline_names = str.splitlines(output.decode("utf-8"))
    return [pid.replace('"', '')
            for pid in pipeline_ids_quoted], [pid.replace('"', '') for pid in pipeline_names]


def list_all_files_mintdt():
    filesnames = glob.glob(f"{TMP_DIR}/*.json")
    docker_names = [fn.split("/")[-1].split(".")[0] for fn in filesnames]
    return docker_names, filesnames
import argparse
import os
from pathlib import Path

from dtran.config_parser import ConfigParser
import re


def exec_dame():
    parser = argparse.ArgumentParser(description="Wrapper for DAME execution")
    parser.add_argument("--config", help="YAML Config file for transformation pipeline")

    args, unknown = parser.parse_known_args()

    for arg in unknown:
        if arg.startswith(("-", "--")):
            parser.add_argument(arg, type=str)

    args, unknown = parser.parse_known_args()

    f = open(args.config, "r")
    file_content = f.read()
    f.close()

    output_file = ""

    for key, value in vars(args).items():
        if value is None:
            value = "null"
        if re.match("^p[0-9]+$", key):
            param = key.replace("p", "PARAMS")
            file_content = file_content.replace(f"{{{param}}}", value)
        elif re.match("^o[0-9]+$", key):
            param = key.replace("o", "OUTPUTS")
            file_content = file_content.replace(f"{{{param}}}", value)
            output_file = value
        elif re.match("^i[0-9]+$", key):
            param = key.replace("i", "INPUTS")
            file_content = file_content.replace(f"{{{param}}}", value)

    new_config_file = args.config.replace(".template", "")
    f = open(new_config_file, "w")
    f.write(file_content)
    f.close()

    parser = ConfigParser()
    parsed_pipeline, parsed_inputs = parser.parse(path=new_config_file)

    # Execute the pipeline
    parsed_pipeline.exec(parsed_inputs)

    os.remove(new_config_file)


if __name__ == '__main__':
    exec_dame()

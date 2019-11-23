import sys

import ujson as json

from .dcat_api import DCatAPI, StandardVariableForm

id = "e8287ea4-e6f2-47aa-8bfc-0c22852735c8"
dcat_api = DCatAPI.get_instance()


def register_dataset(metadata):
    for obj in metadata:
        description = obj["description"]
        name = obj["name"]
        url = obj["url"]
        file_type = url.split(".")[-1]
        start_time = obj["start_time"]
        end_time = obj["end_time"]
        spatial_coverage = obj["bounding_box"]

        sv_forms = []

        for sv in obj["standard_variables"]:
            sv_form = StandardVariableForm(sv["ontology"], sv["name"], sv["uri"])
            sv_forms.append(sv_form)

        ids = dcat_api.register_variables(sv_forms)
        variables = [{"name": f"var_{idx}", "standard_name_id": [id]} for idx, id in enumerate(ids)]

        resources_metadata = [
            {
                "data_url": file_id,
                "name": file_id.rsplit("/", maxsplit=1)[-1],
                "resource_type": file_type.lower(),
                "metadata": {
                    "spatial_coverage": {
                        "type": "BoundingBox",
                        "value": {
                            "xmin": spatial_coverage[0],
                            "ymin": spatial_coverage[1],
                            "xmax": spatial_coverage[2],
                            "ymax": spatial_coverage[3],
                        },
                    },
                    "temporal_coverage": {
                        "start_time": start_time,
                        "end_time": end_time
                    },
                },
            }
            for idx, file_id in enumerate([url])
        ]

        return dcat_api.register_dataset_with_multiple_resources(
            id, name, description, variables, resources_metadata
        )


if __name__ == "__main__":
    args = sys.argv[1:]

    metadata_objs = json.load(args[0])

    print(register_dataset(metadata_objs))

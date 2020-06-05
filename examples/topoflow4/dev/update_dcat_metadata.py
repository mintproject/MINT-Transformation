import os

from dtran.dcat.api import DCatAPI

if __name__ == '__main__':
    # Update GLDAS representation
    provenance_id = os.environ['PROVENANCE_ID']
    dsmodel = "/home/rook/workspace/mint/MINT-Transformation/examples/topoflow4/dev/gldas.yml"
    dataset_id = "5babae3f-c468-4e01-862e-8b201468e3b5"
    from ruamel.yaml import YAML

    yaml = YAML()
    with open(dsmodel, "r") as f:
        dsmodel = yaml.load(f)

    with open("/home/rook/Downloads/metadata.json", "w") as f:
        import json
        json.dump({
            "resource_repr": dsmodel
        }, f)

    DCatAPI.get_instance().update_dataset_metadata(provenance_id, dataset_id, {
        "resource_repr": dsmodel
    })
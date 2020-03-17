from dtran.dcat.api import DCatAPI
import click
import json

PROVENANCE_ID = "b3e79dc2-8fa1-4203-ac82-b5267925191f"


@click.group(invoke_without_command=False)
def cli():
    pass


@cli.command(name="register_dataset", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=False,
))
@click.option("--name", help="DCAT dataset name", default="test-dataset")
@click.option("--description", help="DCAT dataset description", default="test-description")
@click.option("--metadata_path", help="DCAT dataset metadata file path", default=None)
@click.option("--resource_path", help="DCAT dataset resources json path, should be a file name-url dict", default=None)
@click.option("--resource_type", help="DCAT dataset resource type", default="zip")
@click.option("--variable_path", help="DCAT dataset variable json path", default=None)
def register_dataset(name, description, metadata_path, resource_path, variable_path, resource_type):
    """
    Registers DCAT dataset with multiple resources.
    Example: PYTHONPATH=$(pwd):$(pwd):$PYTHONPATH python dtran/dcat/scripts/register_datasets.py register_dataset
    --resource_path=./uploaded.json --variable_path=variables.json
    """

    dcat = DCatAPI.get_instance("https://api.mint-data-catalog.org")

    if metadata_path is None:
        metadata = {}
    else:
        with open(metadata_path, "r") as f:
            metadata = json.load(f)

    if resource_path is None:
        resource_data = {}
    else:
        with open(resource_path, "r") as f:
            resource_data = json.load(f)

    if variable_path is None:
        variable_data = {}
    else:
        with open(variable_path, "r") as f:
            variable_data = json.load(f)

    # register variables
    variables = dcat.register_special_variables(variable_data)

    # create resource metadata
    resources_metadata = [
        {
            "data_url": file_url,
            "name": file_name,
            "resource_type": resource_type.lower(),
            "metadata": {},
        }
        for file_name, file_url in resource_data.items()
    ]

    dataset, resources, variables = dcat.register_dataset_with_multiple_resources(
        PROVENANCE_ID,
        name,
        description,
        variables,
        resources_metadata,
        dataset_metadata=metadata,
    )
    print(f"dataset is {dataset}")
    print(f"resources is {resources}")
    print(f"variables is {variables}")


@cli.command(name="delete_dataset", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=False,
))
@click.option("--dcatid", help="DCAT dataset ID", default=None)
@click.option("--json_path", help="A json file containing DCAT dataset ids as a list", default=None)
def delete_dataset(dcatid, json_path):
    """
    Delete specified datasets.
    Example: PYTHONPATH=$(pwd):$(pwd):$PYTHONPATH python dtran/dcat/scripts/register_datasets.py delete_dataset --dcatid=c4fedf48-f888-4de1-b60f-c6ac5cb1615b
    """
    dcat = DCatAPI.get_instance("https://api.mint-data-catalog.org")

    if dcatid is None and json_path is None:
        raise ValueError("Please enter dataset ids to delete!")

    if json_path is None:
        dcat_ids = [dcatid]
    else:
        with open(json_path, "r") as f:
            dcat_ids = json.load(f)

    assert type(dcat_ids) is list, f"json file should be a list of DCAT dataset ids!"

    deleted_ids = dcat.delete_datasets(
        provenance_id=PROVENANCE_ID,
        dataset_ids=dcat_ids
    )
    print(f"Deleted {deleted_ids}.")


if __name__ == "__main__":
    cli()

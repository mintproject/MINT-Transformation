from dcatreg.dcat_api import DCatAPI

if __name__ == "__main__":
    id = "e8287ea4-e6f2-47aa-8bfc-0c22852735c8"
    dcat_api = DCatAPI.get_instance()

    dcat_api.delete_datasets(
        id,
        [
            "d217ca79-f67f-4628-ade3-814c691071e7",
        ],
    )

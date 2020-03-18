import click
import subprocess
import os
import owncloud
import json
import glob


def setup_owncloud(upload_dir):
    oc = owncloud.Client('https://files.mint.isi.edu/')
    oc.login('datacatalog', 'sVMIryVWEx3Ec2')
    oc.mkdir(upload_dir)
    return oc


def upload_to_mint_server(target_dir, target_filename, upload_url):
    upload_output = subprocess.check_output(
        f"curl -sD - --user upload:HVmyqAPWDNuk5SmkLOK2 --upload-file {target_dir}/{target_filename} {upload_url}",
        shell=True,
    )
    uploaded_url = f'https://{upload_output.decode("utf-8").split("https://")[-1]}'
    return target_filename, uploaded_url


def upload_to_owncloud_server(oc, target_dir, target_filename, upload_dir):
    print(f"uploading {target_dir}/{target_filename}...")
    oc.put_file(f'{upload_dir}/{target_filename}', f"{target_dir}/{target_filename}")

    link_info = oc.share_file_with_link(f'{upload_dir}/{target_filename}')

    return target_filename, link_info.get_link()


@click.group(invoke_without_command=False)
def cli():
    pass


@cli.command(name="upload_files", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=False,
))
@click.option("--server", help="server type: WINGS or OWNCLOUD", default="OWNCLOUD")
@click.option("--dir", help="directory of files to be uploaded", default=".")
@click.option("--ext", help="file extension of files to be uploaded", default="zip")
@click.option("--upload_dir", help="uploaded file directory", default="test-dir")
def upload_files(server, dir, ext, upload_dir):
    """
    Uploads specified files and return a dictionary of their uploaded url.
    Example: python dtran/dcat/scripts/upload_files_in_batch.py upload_files --server=OWNCLOUD --dir=. --ext=zip --upload_dir=xt-test
    """
    if server not in ["OWNCLOUD", "WINGS"]:
        raise ValueError(f"Should input values: 'WINGS' for MINT Publisher and 'OWNCOULD' for mint data bucket! \
                          Your input: {server} is invalid!")

    if not upload_dir:
        raise ValueError(f"upload_dir should not be empty! Please enter an upload destination!")

    files = glob.glob(f"{dir}/*.{ext}")
    file_names = [os.path.basename(fn) for fn in files]
    res = {}

    if server == "OWNCLOUD":
        oc = setup_owncloud(upload_dir)
        for fn in file_names:
            filename, file_link = upload_to_owncloud_server(
                oc, dir, fn, upload_dir
            )
            res[filename] = f"{file_link}/download"
    else:
        upload_url = "https://publisher.mint.isi.edu"
        for fn in file_names:
            filename, file_link = upload_to_mint_server(
                dir, fn, upload_url
            )
            res[filename] = f"{file_link}"
    with open("./uploaded.json", "w") as f:
        json.dump(res, f, indent=4)


if __name__ == "__main__":
    cli()
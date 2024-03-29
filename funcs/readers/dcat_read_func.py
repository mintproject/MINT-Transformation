#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import os
import shutil
import subprocess
import time
import requests
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Union, Dict
from functools import partial
from playhouse.kv import KeyValue
from peewee import SqliteDatabase, Model, UUIDField, IntegerField, BooleanField, BigIntegerField, DoesNotExist

from drepr import DRepr
from drepr.outputs import ArrayBackend, GraphBackend
from dtran.argtype import ArgType
from dtran.backend import ShardedBackend, ShardedClassID, LazyLoadBackend
from dtran.dcat.api import DCatAPI
from dtran.ifunc import IFunc, IFuncType

DATA_CATALOG_DOWNLOAD_DIR = os.path.abspath(os.environ["DATA_CATALOG_DOWNLOAD_DIR"])
if os.environ['NO_CHECK_CERTIFICATE'].lower().strip() == 'true':
    DOWNLOAD_CMD = "wget --no-check-certificate"
else:
    DOWNLOAD_CMD = "wget"

Path(DATA_CATALOG_DOWNLOAD_DIR).mkdir(exist_ok=True, parents=True)

UNITS_MAPPING = {
    'PB': 1 << 50,
    'TB': 1 << 40,
    'GB': 1 << 30,
    'MB': 1 << 20,
    'KB': 1 << 10,
    'B': 1
}


class Resource(Model):
    resource_id = UUIDField(unique=True)
    ref_count = IntegerField(default=0, index=True)
    is_downloading = BooleanField(default=False)
    size = BigIntegerField(default=0)

    class Meta:
        database = SqliteDatabase(os.path.join(DATA_CATALOG_DOWNLOAD_DIR, 'dcat_read_func.db'), timeout=10)


class ResourceManager:
    instance = None

    def __init__(self):
        self.max_capacity = 200 * UNITS_MAPPING['MB']
        self.max_clear_size = 100 * UNITS_MAPPING['MB']
        assert self.max_capacity >= self.max_clear_size, "max_capacity cannot be less than max_clear_size"
        self.poll_interval = 10
        self.compressed_resource_types = {".zip", ".tar.gz", ".tar"}
        self.db = Resource._meta.database
        self.db.connect()
        self.db.create_tables([Resource], safe=True)
        self.db.close()
        self.kv = KeyValue(database=self.db, value_field=BigIntegerField())
        with self.db.atomic('EXCLUSIVE'):
            # initializing current_size of DATA_CATALOG_DOWNLOAD_DIR in the database
            if 'current_size' not in self.kv:
                self.kv['current_size'] = sum(f.stat().st_size for f in Path(DATA_CATALOG_DOWNLOAD_DIR).rglob('*'))
            else:
                for resource in Resource.select():
                    if resource.is_downloading:
                        continue
                    path = Path(os.path.join(DATA_CATALOG_DOWNLOAD_DIR, str(resource.resource_id) + '.dat'))
                    if not path.exists():
                        path = Path(os.path.join(DATA_CATALOG_DOWNLOAD_DIR, str(resource.resource_id)))
                        if not path.exists():
                            self.kv['current_size'] -= resource.size
                            resource.delete_instance()

    @staticmethod
    def get_instance():
        if ResourceManager.instance is None:
            ResourceManager.instance = ResourceManager()
        return ResourceManager.instance

    def download(self, resource_id: str, resource_metadata: Dict[str, str], should_redownload: bool) -> str:
        is_compressed = resource_metadata['resource_type'] in self.compressed_resource_types
        if is_compressed:
            if resource_metadata['resource_type'].startswith(".tar"):
                raise NotImplementedError()
            path = os.path.join(DATA_CATALOG_DOWNLOAD_DIR, resource_id)
        else:
            path = os.path.join(DATA_CATALOG_DOWNLOAD_DIR, resource_id + '.dat')

        download = True
        with self.db.atomic('EXCLUSIVE'):
            try:
                # if resource already exists
                resource = Resource.select().where(Resource.resource_id == resource_id).get()
                resource.ref_count += 1
                if not should_redownload:
                    # TODO: comparing timestamp before skipping download
                    download = False
            except DoesNotExist:
                resource = Resource.create(resource_id=resource_id, ref_count=1, is_downloading=True, size=0)

            if download:
                required_size = 0
                if resource.ref_count > 1:
                    # adjust required_size when the resource is to be redownloaded
                    required_size -= resource.size
                try:
                    resource.size = int(requests.head(resource_metadata['resource_data_url']).headers['Content-Length'])
                    required_size += resource.size
                except KeyError:
                    pass
                if self.max_capacity - self.kv['current_size'] < required_size:
                    # clear files to make space
                    self.kv['current_size'] -= self.clear()
                    assert self.max_capacity - self.kv['current_size'] >= required_size, "Not enough disk space"
                self.kv['current_size'] += required_size
            resource.save()

        if download:
            if resource.ref_count > 1:
                # block until all other processes accessing the resource are finished
                DcatReadFunc.logger.debug(f"Waiting for some other process/thread to free resource {resource_id} ...")
                while resource.ref_count > 1:
                    time.sleep(self.poll_interval)
                    with self.db.atomic('EXCLUSIVE'):
                        resource = Resource.select().where(Resource.resource_id == resource_id).get()
                        if resource.ref_count == 1:
                            # setting is_downloading before redownload
                            resource.is_downloading = True
                            resource.save()
                # clear old resource before redownload
                if is_compressed:
                    shutil.rmtree(str(path))
                else:
                    Path(path).unlink()
            DcatReadFunc.logger.debug(f"Downloading resource {resource_id} ...")
            if is_compressed:
                temp_path = path + resource_metadata['resource_type']
                subprocess.check_call(f"wget -q \"{resource_metadata['resource_data_url']}\" -O {temp_path}", shell=True, close_fds=False)
                self.uncompress(resource_metadata['resource_type'], path)
                # adjust required_size when the resource is compressed
                required_size = -resource.size
                required_size += sum(f.stat().st_size for f in Path(path).rglob('*')) + Path(path).stat().st_size
                Path(temp_path).unlink()
            else:
                subprocess.check_call(f"wget -q \"{resource_metadata['resource_data_url']}\" -O {path}", shell=True, close_fds=False)
                required_size = 0

            with self.db.atomic('EXCLUSIVE'):
                self.kv['current_size'] += required_size
                resource = Resource.select().where(Resource.resource_id == resource_id).get()
                resource.size += required_size
                resource.is_downloading = False
                resource.save()
        else:
            DcatReadFunc.logger.debug(f"Skipping resource {resource_id}, found in cache")
            if resource.is_downloading:
                # block until some other process is done downloading the resource
                DcatReadFunc.logger.debug(f"Waiting for other process/thread to finish downloading resource {resource_id} ...")
                while resource.is_downloading:
                    time.sleep(self.poll_interval)
                    with self.db.atomic('EXCLUSIVE'):
                        resource = Resource.select().where(Resource.resource_id == resource_id).get()

        return self.path(resource_id, path, is_compressed)

    def unlink(self, resource_id):
        with self.db.atomic('EXCLUSIVE'):
            resource = Resource.select().where(Resource.resource_id == resource_id).get()
            resource.ref_count -= 1
            resource.save()

    def clear(self) -> int:
        size = 0
        for resource in Resource.select().where(Resource.ref_count == 0).order_by(Resource.id):
            DcatReadFunc.logger.debug(f"Clearing resource {resource.resource_id}")
            path = Path(os.path.join(DATA_CATALOG_DOWNLOAD_DIR, str(resource.resource_id) + '.dat'))
            if path.exists():
                size += path.stat().st_size
                path.unlink()
            else:
                path = Path(os.path.join(DATA_CATALOG_DOWNLOAD_DIR, str(resource.resource_id)))
                size += sum(f.stat().st_size for f in path.rglob('*')) + path.stat().st_size
                shutil.rmtree(str(path))
            resource.delete_instance()
            if size >= self.max_clear_size:
                return size
        return size

    def uncompress(self, resource_type: str, path: Union[Path, str]):
        subprocess.check_call(f"unzip {path + resource_type} -d {path}", shell=True, close_fds=False)
        # flatten the structure (max two levels)
        for fpath in Path(path).iterdir():
            if fpath.is_dir():
                for sub_file in fpath.iterdir():
                    new_file = os.path.join(path, sub_file.name)
                    if os.path.exists(new_file):
                        raise Exception("Invalid resource. Shouldn't overwrite existing file")
                    os.rename(str(sub_file), new_file)
                shutil.rmtree(str(fpath))

    def path(self, resource_id: str, path: Union[Path, str], is_compressed: bool) -> str:
        if not is_compressed:
            return path
        # we need to look in the folder and find the resource
        files = [
            fpath for fpath in Path(path).iterdir()
            if fpath.is_file() and not fpath.name.startswith(".")
        ]
        if len(files) == 0:
            raise Exception(f"The compressed resource {resource_id} is empty")
        elif len(files) != 1:
            # this indicates the shapefile
            files = [f for f in files if f.name.endswith(".shp")]
            if len(files) != 1:
                raise Exception(
                    f"Cannot handle compressed resource {resource_id} because it has more than one resource"
                )
        return str(files[0])


class DcatReadFunc(IFunc):
    id = "dcat_read_func"
    description = """ An entry point in the pipeline.
    Fetches a dataset and its metadata from the MINT Data-Catalog.
    """
    func_type = IFuncType.READER
    friendly_name: str = "Data Catalog Reader"
    inputs = {
        "dataset_id": ArgType.String,
        "start_time": ArgType.DateTime(optional=True),
        "end_time": ArgType.DateTime(optional=True),
        "lazy_load_enabled": ArgType.Boolean(optional=True),
        "should_redownload": ArgType.Boolean(optional=True),
        "override_drepr": ArgType.String(optional=True),
    }
    outputs = {"data": ArgType.DataSet(None), "data_path": ArgType.ListString(optional=True)}
    example = {
        "dataset_id": "ea0e86f3-9470-4e7e-a581-df85b4a7075d",
        "start_time": "2020-03-02T12:30:55",
        "end_time": "2020-03-02T12:30:55",
        "lazy_load_enabled": "False",
        "should_redownload": "False",
        "override_drepr": "/tmp/model.yml"
    }
    logger = logging.getLogger(__name__)

    def __init__(self,
                 dataset_id: str,
                 start_time: datetime = None,
                 end_time: datetime = None,
                 lazy_load_enabled: bool = False,
                 should_redownload: bool = False,
                 override_drepr: str = None
                 ):
        self.dataset_id = dataset_id
        self.lazy_load_enabled = lazy_load_enabled
        self.should_redownload = should_redownload
        self.resource_manager = ResourceManager.get_instance()
        dataset = DCatAPI.get_instance().find_dataset_by_id(dataset_id)

        assert ('resource_repr' in dataset['metadata']) or ('dataset_repr' in dataset['metadata']), \
            "Dataset is missing both 'resource_repr' and 'dataset_repr'"
        assert not (('resource_repr' in dataset['metadata']) and ('dataset_repr' in dataset['metadata'])), \
            "Dataset has both 'resource_repr' and 'dataset_repr'"

        resources = DCatAPI.get_instance().find_resources_by_dataset_id(dataset_id, start_time, end_time)

        self.resources = OrderedDict()
        if 'resource_repr' in dataset['metadata']:
            if override_drepr is not None:
                self.drepr = DRepr.parse_from_file(override_drepr)
            else:
                self.drepr = DRepr.parse(dataset['metadata']['resource_repr'])
            for resource in resources:
                self.resources[resource['resource_id']] = {key: resource[key] for key in
                                                           {'resource_data_url', 'resource_type'}}
            self.repr_type = 'resource_repr'
        else:
            # TODO: fix me!!
            assert len(resources) == 1
            self.resources[resources[0]['resource_id']] = {key: resources[0][key] for key in
                                                           {'resource_data_url', 'resource_type'}}
            if override_drepr is not None:
                self.drepr = DRepr.parse_from_file(override_drepr)
            else:
                self.drepr = DRepr.parse(dataset['metadata']['dataset_repr'])
            self.repr_type = 'dataset_repr'

        self.logger.debug(f"Found key '{self.repr_type}'")

    def exec(self) -> dict:
        # TODO: fix me! incorrect way to choose backend
        if self.get_preference("data") is None or self.get_preference("data") == 'array':
            backend = ArrayBackend
        else:
            backend = GraphBackend

        if self.lazy_load_enabled:
            if self.repr_type == 'dataset_repr':
                resource_id, resource_metadata = list(self.resources.items())[0]
                return {"data": LazyLoadBackend(backend, self.drepr, partial(self.resource_manager.download,
                                                                             resource_id, resource_metadata,
                                                                             self.should_redownload),
                                                self.resource_manager.unlink)}
            else:
                dataset = ShardedBackend(len(self.resources))
                for resource_id, resource_metadata in self.resources.items():
                    dataset.add(LazyLoadBackend(backend, self.drepr, partial(self.resource_manager.download,
                                                                             resource_id, resource_metadata,
                                                                             self.should_redownload),
                                                self.resource_manager.unlink, partial(ShardedClassID, dataset.count)))
                return {"data": dataset}
        else:
            # data_path is location of the resources in disk, for pipeline that wants to download the file
            if self.repr_type == 'dataset_repr':
                resource_id, resource_metadata = list(self.resources.items())[0]
                resource_file = self.resource_manager.download(resource_id, resource_metadata, self.should_redownload)
                return {"data": backend.from_drepr(self.drepr, resource_file), "data_path": [resource_file]}
            else:
                dataset = ShardedBackend(len(self.resources))
                data_path = []
                for resource_id, resource_metadata in self.resources.items():
                    resource_file = self.resource_manager.download(resource_id, resource_metadata, self.should_redownload)
                    dataset.add(backend.from_drepr(self.drepr, resource_file, dataset.inject_class_id))
                    data_path.append(data_path)
                return {"data": dataset, "data_path": data_path}

    def __del__(self):
        if not self.lazy_load_enabled:
            for resource_id, resource_metadata in self.resources.items():
                self.resource_manager.unlink(resource_id)

    def validate(self) -> bool:
        return True

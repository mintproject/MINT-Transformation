#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, numpy as np
import logging
import time
import uuid

import requests
import enum
from datetime import datetime, timezone
from dataclasses import dataclass
from dateutil import parser
from typing import Union, List, Dict, Iterable
from pathlib import Path
from drepr import DRepr, outputs
from drepr.executors.readers.np_dict import NPDictReader
from drepr.executors.readers.reader_container import ReaderContainer
from drepr.models import SemanticModel
from drepr.outputs import ArrayBackend, GraphBackend
from drepr.outputs.base_lst_output_class import BaseLstOutputClass
from drepr.outputs.base_output_class import BaseOutputClass
from drepr.outputs.base_output_sm import BaseOutputSM
import subprocess

from drepr.outputs.base_record import BaseRecord
from drepr.outputs.record_id import RecordID

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from funcs.readers.dcat_read_func import ShardedBackend


@dataclass
class GroupByProp:
    prop: str
    value: str

    def timestamp2key(self, value):
        if self.value == "exact":
            return int(value * 1000)
        else:
            dt = datetime.fromtimestamp(value, tz=timezone.utc)
            if self.value == "minute":
                dt = dt.replace(minute=0, second=0, microsecond=0)
            elif self.value == "hour":
                dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            elif self.value == "date":
                dt = dt.replace(day=0, hour=0, minute=0, second=0, microsecond=0)
            elif self.value == "month":
                dt = dt.replace(month=0, day=0, hour=0, minute=0, second=0, microsecond=0)
            elif self.value == "year":
                dt = dt.replace(year=0, month=0, day=0, hour=0, minute=0, second=0, microsecond=0)
            return int(dt.timestamp() * 1000)

    def key2timestamp(self, key):
        return float(key / 1000)


@dataclass
class GroupBy:
    group_props: List[GroupByProp]


class AggregationFunc(enum.Enum):
    SUM = "sum"
    AVG = "average"
    COUNT = "count"


class VariableAggregationFunc(IFunc):
    id = "aggregation_func"
    description = ''''''
    func_type = IFuncType.AGGREGATION_TRANS
    friendly_name: str = "Aggregation Function"
    inputs = {
        "dataset": ArgType.DataSet(None),
        "group_bys": ArgType.VarAggGroupBy,
        "operator": ArgType.VarAggFunc
    }
    outputs = {"data": ArgType.DataSet(None)}
    example = {
        "group_by": "time, lat, long, place",
        "operator": "count, sum, avg"
    }
    logger = logging.getLogger(__name__)

    def __init__(self, dataset, group_by, operator):
        self.dataset = dataset
        self.group_by = GroupBy([GroupByProp(**x) for x in group_by])
        self.operator = AggregationFunc(operator)

    def exec(self) -> dict:
        output = {}
        values = []

        if isinstance(self.dataset, ShardedBackend):
            # check if the data is partition
            for dataset in reversed(self.dataset.datasets):
                values += VariableAggregationFunc._aggregate(dataset, self.group_by, self.operator)
        else:
            values = VariableAggregationFunc._aggregate(self.dataset, self.group_by, self.operator)

        if len(values) > 1:
            ds = ShardedBackend(len(values))
            for v in values:
                resource_id = "resource-" + str(uuid.uuid4())
                reader = NPDictReader(v['data'])
                ReaderContainer.get_instance().set(resource_id, reader)

                dsmodel = {
                    "version": "2",
                    "resources": "container",
                    "attributes": v['attrs'],
                    "alignments": v['aligns'],
                    "semantic_model": v['sm']
                }
                dsmodel = DRepr.parse(dsmodel)
                ds.add(outputs.ArrayBackend.from_drepr(dsmodel, resource_id, ds.inject_class_id))
                ReaderContainer.get_instance().delete(resource_id)
        else:
            for v in values:
                resource_id = "resource-" + str(uuid.uuid4())
                reader = NPDictReader(v['data'])
                ReaderContainer.get_instance().set(resource_id, reader)

                dsmodel = {
                    "version": "2",
                    "resources": "container",
                    "attributes": v['attrs'],
                    "alignments": v['aligns'],
                    "semantic_model": v['sm']
                }
                dsmodel = DRepr.parse(dsmodel)
                ds = outputs.ArrayBackend.from_drepr(dsmodel, resource_id)
        return {"data": ds}

    def validate(self) -> bool:
        return True

    @staticmethod
    def _aggregate(sm, group_by: GroupBy, func: AggregationFunc):
        """Aggregate the data"""
        # TODO: fix me, this is currently implement a corner case
        rdf = sm.ns(outputs.Namespace.RDF)
        mint_geo = sm.ns("https://mint.isi.edu/geo")
        mint = sm.ns("https://mint.isi.edu/")
        groups = {}
        for c in sm.c(mint.Variable):
            index_keys = [p.prop for p in group_by.group_props if c.p(p.prop).ndarray_size() != 1]
            first_record = next(c.iter_records())
            sub_key = tuple([
                first_record.s(p.prop) if c.p(p.prop).ndarray_size() == 1 else None
                for p in group_by.group_props
            ])
            assert 'mint:timestamp' not in index_keys
            if sub_key not in groups:
                groups[sub_key] = {
                    "key": sub_key,
                    "record": first_record,
                    "carried_props": [
                        p for p, po in c.predicates.items() if po.ndarray_size() == 1
                    ],
                    "index_props": index_keys,
                    "data": [c.p(rdf.value).as_ndarray([c.p(x) for x in index_keys])]
                }
            else:
                groups[sub_key]['data'].append(
                    c.p(rdf.value).as_ndarray([c.p(x) for x in index_keys]))

        raw_ds = []
        for key, group in groups.items():
            # because the rest is group by exact value, we don't need to do anything
            values = group['data']
            if func == AggregationFunc.SUM:
                total = np.zeros_like(values[0].data)
                for v in values:
                    total += v.data * (v.data != v.nodata.value)
                result = total
            elif func == AggregationFunc.COUNT:
                total = np.zeros_like(values[0].data)
                for v in values:
                    total += (v.data != v.nodata.value).astype(np.int64)
                result = total
            elif func == AggregationFunc.AVG:
                total = np.zeros_like(values[0].data)
                n_obs = np.zeros_like(values[0].data)
                for v in values:
                    mask = (v.data != v.nodata.value).astype(np.float32)
                    n_obs += mask
                    total += v.data * mask

                obs_mask = n_obs != 0
                result = np.zeros_like(values[0].data)
                result[obs_mask] = total[obs_mask] / n_obs[obs_mask]

            attrs = {'rdf_value': "$.rdf_value" + ("[:]" * len(result.shape))}
            aligns = []
            tbl = {}
            raw_sm = {
                "mint:Variable:1": {
                    "properties": [
                        ("rdf:value", "rdf_value"),
                    ],
                    'links': []
                },
                "prefixes": {
                    "mint": "https://mint.isi.edu/",
                    "mint-geo": "https://mint.isi.edu/geo"
                }
            }
            tbl['rdf_value'] = result
            for p in group['carried_props']:
                if p in {"mint:place", "mint-geo:raster"}:
                    o = sm.get_record_by_id(group['record'].s(p))
                    if p == "mint-geo:raster":
                        raw_sm['mint-geo:Raster:1'] = {"properties": []}
                        raw_sm['mint:Variable:1']['links'].append(
                            ('mint-geo:raster', 'mint-geo:Raster:1'))
                        for k, v in o.to_dict().items():
                            if k == '@id':
                                continue

                            aid = f"{p}_{k}".replace(":", "_")
                            raw_sm['mint-geo:Raster:1']['properties'].append((k, aid))
                            tbl[aid] = v[0]
                            attrs[aid] = f"$.{aid}"
                            aligns.append({
                                "type": "dimension",
                                "source": "rdf_value",
                                "target": aid,
                                "aligned_dims": []
                            })
                    elif p == 'mint:place':
                        raw_sm['mint:Place:1'] = {"properties": []}
                        raw_sm['mint:Variable:1']['links'].append(('mint:place', 'mint:Place:1'))
                        for k, v in o.to_dict().items():
                            if k == '@id':
                                continue

                            if k.startswith("mint:"):
                                aid = f"{p}_{k}".replace(":", "_")
                                tbl[aid] = v[0]
                                attrs[aid] = f"$.{aid}"
                                raw_sm['mint:Place:1']['properties'].append((k, aid))
                                aligns.append({
                                    "type": "dimension",
                                    "source": "rdf_value",
                                    "target": aid,
                                    "aligned_dims": []
                                })
                else:
                    aid = p.replace(":", "_")
                    tbl[aid] = group['record'].s(p)
                    attrs[aid] = f"$.{aid}"
                    raw_sm['mint:Variable:1']['properties'].append((p, aid))
                    aligns.append({
                        "type": "dimension",
                        "source": 'rdf_value',
                        "target": aid,
                        "aligned_dims": []
                    })
            for i, p in enumerate(group['index_props']):
                aid = p.replace(":", "_")
                tbl[aid] = values[0].index_props[i]
                attrs[aid] = f"$.{aid}[:]"
                raw_sm['mint:Variable:1']['properties'].append((p, aid))
                aligns.append({
                    "type": "dimension",
                    "source": "rdf_value",
                    "target": aid,
                    "aligned_dims": [{
                        "source": i + 1,
                        "target": 1
                    }]
                })

            raw_ds.append({"data": tbl, "attrs": attrs, "aligns": aligns, "sm": raw_sm})
        return raw_ds

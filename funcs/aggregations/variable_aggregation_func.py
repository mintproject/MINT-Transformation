#!/usr/bin/python
# -*- coding: utf-8 -*-

import enum
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import chain
from typing import List

import numpy as np
from drepr import DRepr, outputs
from drepr.executors.readers.np_dict import NPDictReader
from drepr.executors.readers.reader_container import ReaderContainer

from dtran.argtype import ArgType
from dtran.ifunc import IFunc, IFuncType
from funcs.readers.dcat_read_func import ShardedBackend


@dataclass
class GroupByProp:
    prop: str
    value: str

    def to_key(self, value):
        if self.prop == "mint:timestamp":
            if self.value == "exact":
                return int(value * 1000)
            else:
                dt = datetime.fromtimestamp(value, tz=timezone.utc)
                if self.value == "minute":
                    dt = dt.replace(second=0, microsecond=0)
                elif self.value == "hour":
                    dt = dt.replace(minute=0, second=0, microsecond=0)
                elif self.value == "day":
                    dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
                elif self.value == "month":
                    dt = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                elif self.value == "year":
                    dt = dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                return int(dt.timestamp() * 1000)
        else:
            return value

    def from_key(self, key):
        if self.prop == "mint:timestamp":
            return float(key / 1000)
        else:
            return key


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
        "group_by": ArgType.VarAggGroupBy,
        "function": ArgType.VarAggFunc
    }
    outputs = {"data": ArgType.DataSet(None)}
    example = {
        "group_by": "time, lat, long, place",
        "function": "count, sum, average"
    }
    logger = logging.getLogger(__name__)

    def __init__(self, dataset, group_by, function):
        self.dataset = dataset
        self.group_by = GroupBy([GroupByProp(**x) for x in group_by])
        self.function = AggregationFunc(function)

    def exec(self) -> dict:
        groups = {}

        if isinstance(self.dataset, ShardedBackend):
            # check if the data is partition
            for dataset in reversed(self.dataset.datasets):
                VariableAggregationFunc._group_by(dataset, self.group_by, groups)
        else:
            VariableAggregationFunc._group_by(self.dataset, self.group_by, groups)

        values = VariableAggregationFunc._aggregate(groups, self.function)

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
    def _group_by(sm, group_by: GroupBy, groups: dict):
        rdf = sm.ns(outputs.Namespace.RDF)
        mint_geo = sm.ns("https://mint.isi.edu/geo")
        mint = sm.ns("https://mint.isi.edu/")
        for c in sm.c(mint.Variable):
            index_keys = [p.prop for p in group_by.group_props if c.p(p.prop).ndarray_size() != 1]
            first_record = next(c.iter_records())
            key_props = [p for p in group_by.group_props if c.p(p.prop).ndarray_size() == 1]
            sub_key = tuple([
                p.to_key(first_record.s(p.prop)) for p in key_props
            ])
            assert 'mint:timestamp' not in index_keys
            if sub_key not in groups:
                groups[sub_key] = {
                    "key": sub_key,
                    "dataset": sm,
                    "record": first_record,
                    "key_props": key_props,
                    "carried_props": [
                        p for p, po in c.predicates.items() if po.ndarray_size() == 1
                    ],
                    "index_props": index_keys,
                    "data": [c.p(rdf.value).as_ndarray([c.p(x) for x in index_keys])]
                }
            else:
                groups[sub_key]['data'].append(
                    c.p(rdf.value).as_ndarray([c.p(x) for x in index_keys]))

    @staticmethod
    def _aggregate(groups: dict, func: AggregationFunc):
        """Aggregate the data"""
        # TODO: fix me, this is currently implement a corner case
        raw_ds = []
        for key, group in groups.items():
            # because the rest is group by exact value, we don't need to do anything
            values = group['data']
            if func == AggregationFunc.SUM:
                total = np.zeros_like(values[0].data)
                for v in values:
                    total += v.data * (v.data != v.nodata.value)
                if len(group['index_props']) < len(values[0].data.shape):
                    # one extra dimension at the end, which we need to sum
                    total = total.sum(axis=-1)
                result = total
            elif func == AggregationFunc.COUNT:
                total = np.zeros_like(values[0].data)
                for v in values:
                    total += (v.data != v.nodata.value).astype(np.int64)

                if len(group['index_props']) < len(values[0].data.shape):
                    # one extra dimension at the end, which we need to sum
                    total = total.sum(axis=-1)
                result = total
            elif func == AggregationFunc.AVG:
                total = np.zeros_like(values[0].data)
                n_obs = np.zeros_like(values[0].data)
                for v in values:
                    mask = (v.data != v.nodata.value).astype(np.float32)
                    n_obs += mask
                    total += v.data * mask

                if len(group['index_props']) < len(values[0].data.shape):
                    # one extra dimension at the end
                    # calculate the total
                    total = total.sum(axis=-1)
                    # calculate the n_obs
                    n_obs = n_obs.sum(axis=-1)

                    if len(values[0].data.shape) == 1:
                        if n_obs == 0:
                            result = [values[0].nodata.value]
                        else:
                            result = [total / n_obs]
                        result = np.asarray(result)
                    else:
                        result = np.zeros(values[0].data.shape[:-1], dtype=np.float32 if values[0].data.dtype == np.float32 else np.float64)
                        result = total / n_obs
                        result[n_obs == 0] = values[0].nodata.value
                else:
                    obs_mask = n_obs != 0
                    result = np.zeros(values[0].data.shape, dtype=np.float32 if values[0].data.dtype == np.float32 else np.float64)
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
            key_props = {p.prop: i for i, p in enumerate(group['key_props'])}
            for p in group['carried_props']:
                if p in {"mint:place", "mint-geo:raster"}:
                    o = group['dataset'].get_record_by_id(group['record'].s(p))
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
                    if p in key_props:
                        tbl[aid] = group['key_props'][key_props[p]].from_key(group['key'][key_props[p]])
                    else:
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

            # remove raster if we don't have it any more
            has_mintgeo_coor = False
            for prop in chain(raw_sm['mint:Variable:1']['properties'], raw_sm['mint:Variable:1'].get('static_properties', [])):
                if prop[0] == 'mint-geo:lat':
                    has_mintgeo_coor = True
            if not has_mintgeo_coor:
                delete_link = []
                for i, link in enumerate(raw_sm['mint:Variable:1']['links']):
                    if link[0] == 'mint-geo:raster':
                        raw_sm.pop(link[1])
                        delete_link.append(i)
                for i in reversed(delete_link):
                    raw_sm['mint:Variable:1']['links'].pop(i)
            raw_ds.append({"data": tbl, "attrs": attrs, "aligns": aligns, "sm": raw_sm})
        return raw_ds

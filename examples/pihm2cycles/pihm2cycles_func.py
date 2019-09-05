import json
from datetime import datetime, timedelta
from uuid import uuid4

from drepr import Graph
from drepr.graph import Node

from dtran import IFunc, ArgType


class Pihm2CyclesFunc(IFunc):
    id = "pihm2cycles_func"
    inputs = {
        "pihm_data_graph": ArgType.Graph(None),
        "pihm_soil_graph": ArgType.Graph(None),
        "pid_graph": ArgType.Graph(None),
        "cycles_layers": ArgType.String,
        "patch_id": ArgType.Number,
        "gw_depth": ArgType.Number,
    }
    outputs = {"reinit_graph": ArgType.Graph(None), "cycle_soil_graph": ArgType.Graph(None)}

    def __init__(
            self,
            pihm_data_graph: Graph,
            pihm_soil_graph: Graph,
            pid_graph: Graph,
            cycles_layers: str,
            patch_id: int,
            gw_depth: float,
    ):
        self.pihm_data_graph = pihm_data_graph
        self.pihm_soil_graph = pihm_soil_graph
        self.pid_graph = pid_graph

        self.cycles_layers = json.loads(cycles_layers)

        self.patch_id = patch_id
        self.gw_depth = gw_depth

    def exec(self) -> dict:
        cycles_layers_graph = self._soil_transform()
        reinit_graph = self._reinit_transform(cycles_layers_graph)
        return {"reinit_graph": reinit_graph, "cycle_soil_graph": cycles_layers_graph}

    def _reinit_transform(self, cycles_layers_graph):
        reinit_nodes = []
        for pihm_node in self.pihm_data_graph.nodes:
            if "mint:groundWater" in pihm_node.data:
                for cycle_node in cycles_layers_graph.nodes:
                    reinit_node = Node(len(reinit_nodes), {}, [], [])
                    reinit_node.data["@type"] = "cycle:Variable"

                    reinit_node.data["cycle:var_name"] = "SATURATION_L%s" % cycle_node.data["cycle:layer_id"]
                    reinit_node.data["cycle:value"] = self._calculate_saturation(
                        float(cycle_node.data["cycle:top"]),
                        float(cycle_node.data["cycle:bottom"]),
                        self.gw_depth - float(pihm_node.data["mint:groundWater"]),
                    )
                    reinit_node.data["cycle:rot_year"] = timedelta(
                        minutes=float(pihm_node.data["schema:recordedAt"])).days // 365
                    reinit_node.data["cycle:doy"] = timedelta(
                        minutes=float(pihm_node.data["schema:recordedAt"])).days % 365

                    reinit_nodes.append(reinit_node)

            else:
                reinit_node = Node(len(reinit_nodes), {}, [], [])
                reinit_node.data["@type"] = "cycle:Variable"
                reinit_node.data["cycle:var_name"] = "INFILTRATION%s" % cycle_node.data["cycle:layer_id"]
                reinit_node.data["cycle:value"] = self._calculate_infiltration(
                    float(pihm_node.data["mint:infiltration"])
                )
                reinit_node.data["cycle:rot_year"] = timedelta(
                    minutes=float(pihm_node.data["schema:recordedAt"])).days // 365
                reinit_node.data["cycle:doy"] = timedelta(
                    minutes=float(pihm_node.data["schema:recordedAt"])).days % 365

                reinit_nodes.append(reinit_node)
        return Graph(reinit_nodes, [])

    def validate(self) -> bool:
        return True

    def _soil_transform(self):
        mu_key = self._patch_id2key(self.patch_id)

        for node in self.pihm_soil_graph.nodes:
            if node.data["pihm:mu_key"] == mu_key:
                soil_data = node.data
                break
        else:
            raise ValueError("Patch id cannot be found in soil data")

        previous_offset = 0
        nodes = []

        for i, layer_thickness in enumerate(self.cycles_layers):
            node = Node(i, {}, [], [])
            node.data["@type"] = "cycle:Layer"
            node.data["cycle:layer_id"] = i
            node.data["cycle:thickness"] = layer_thickness
            node.data["cycle:top"] = previous_offset
            node.data["cycle:bottom"] = previous_offset + float(layer_thickness)
            node.data["cycle:clay"] = float(soil_data["pihm:clay"])
            node.data["cycle:sand"] = 100 - float(soil_data["pihm:clay"]) - float(soil_data["pihm:slt"])
            node.data["cycle:om"] = self._calculate_organic_matter(
                previous_offset, previous_offset + float(layer_thickness), float(soil_data["pihm:om"])
            )
            node.data["cycle:bld"] = float(soil_data["pihm:bld"])
            node.data["cycle:fc"] = -999
            node.data["cycle:pwp"] = -999
            node.data["cycle:no3"] = -1
            node.data["cycle:nh4"] = 0.2

            nodes.append(node)
            previous_offset = previous_offset + float(layer_thickness)
        return Graph(nodes, [])

    def _patch_id2key(self, patch_id: int) -> int:
        for node in self.pid_graph.nodes:
            if node.data["cycle:index"] == patch_id:
                return node.data["cycle:ele_id"]

    @staticmethod
    def _calculate_infiltration(infiltration: float):
        return infiltration * 1000

    @staticmethod
    def _calculate_saturation(water_level: float, top: float, bottom: float):
        if water_level > bottom:
            return 0.0
        if water_level < top:
            return 1.0
        return (bottom - water_level) * 1.0 / (bottom - top)

    @staticmethod
    def _calculate_sand_level(clay: float, slt: float):
        return 100 - clay - slt

    @staticmethod
    def _calculate_organic_matter(top: float, bottom: float, pihm_om: float) -> float:
        middle_point = (top + bottom) / 2
        return pihm_om / (1 + (middle_point / 0.43) ** 2)

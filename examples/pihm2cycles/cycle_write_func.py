import csv
from pathlib import Path
from typing import Union

from drepr import Graph

from dtran import ArgType, IFunc


class CyclesWriteFunc(IFunc):
    id = "cycle_write_func"
    inputs = {
        "reinit_graph": ArgType.Graph(None),
        "soil_graph": ArgType.Graph(None),
        "reinit_file": ArgType.FilePath,
        "soil_file": ArgType.FilePath,
    }
    outputs = {"result": ArgType.Boolean}

    def __init__(
        self,
        reinit_graph: Graph,
        soil_graph: Graph,
        reinit_file: Union[str, Path],
        soil_file: Union[str, Path],
    ):
        self.reinit_graph = reinit_graph
        self.soil_graph = soil_graph

        self.reinit_file = Path(reinit_file)
        self.soil_file = Path(soil_file)

    def _write_init_file(self):
        with self.reinit_file.open("w") as f:
            writer = csv.writer(f, delimiter="\t")

            writer.writerow(["ROT_YEAR", "DOY", "VARIABLE", "VALUE"])
            for node in self.reinit_graph.nodes:
                print(node)
                doy = node.data["cycle:doy"]
                rot_year = node.data["cycle:rot_year"]
                var_name = node.data["cycle:var_name"]
                value = node.data["cycle:value"]

                writer.writerow([rot_year, doy, var_name, value])

    def _write_soil_file(self):
        with self.soil_file.open("w") as writer:
            writer.write("CURVE_NUMBER        %d\n" % 66.66)
            writer.write("SLOPE               %d\n" % 1)
            writer.write("TOTAL_LAYERS        %d\n" % 8)
            writer.write("LAYER   THICK   CLAY    SAND    ORGANIC BD      FC      PWP     NO3     NH4     \n")
            layers = []

            for node in self.soil_graph.nodes:
                print(node)
                layer_id = node.data["cycle:layerId"]
                thickness = node.data["cycle:thickness"]
                clay = node.data["cycle:clay"]
                sand = node.data["cycle:sand"]
                om = node.data["cycle:om"]
                bd = node.data["cycle:bd"]
                fc = node.data["cycle:fc"]
                pwp = node.data["cycle:pwp"]
                no3 = node.data["cycle:no3"]
                nh4 = node.data["cycle:nh4"]

                layer = [layer_id, thickness, clay, sand, om, bd, fc, pwp, no3, nh4]
                writer.write("\t".join(layer) + "\n")
                layers.append(layer)
            return layers

    def exec(self):
        self._write_init_file()
        self._write_soil_file()
        return {"result": True}

    def validate(self) -> bool:
        return True

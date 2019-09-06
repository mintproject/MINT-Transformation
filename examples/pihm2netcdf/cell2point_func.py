import subprocess
from pathlib import Path
from typing import Union

from dtran import ArgType, IFunc


class Cell2PointFunc(IFunc):
    id = "cell2point_func"
    inputs = {"cell2point_file": ArgType, "cell_file": ArgType.FilePath, "point_file": ArgType.FilePath}
    outputs = {"point_file": ArgType.FilePath}

    def __init__(self, cell_file: Union[str, Path], point_file: Union[str, Path], cell2point_file):
        self.cell_file = cell_file
        self.point_file = point_file
        self.cell2point_file = cell2point_file

    def exec(self) -> dict:
        subprocess.check_output(
            f"Rscript ${self.cell2point_file} ${self.cell_file} ${self.point_file}", shell=True
        )
        return {"point_file": self.point_file}

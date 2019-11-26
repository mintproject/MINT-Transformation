from typing import List, Dict, Tuple, Callable, Any, Optional

from dtran import IFunc


class GenTopoflow4ClimateData(IFunc):
    id = "gen_topoflow4_climate"
    description = "Generate climate topoflow"

    inputs = {
    }
    outputs = {}

    def __init__(self):
        pass

    def exec(self) -> dict:
        return {}

    def validate(self) -> bool:
        return True
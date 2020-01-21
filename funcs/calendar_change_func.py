from drepr import Graph
from ethiopian_date import EthiopianDateConverter

from dtran import IFunc, ArgType


class CalendarChangeFunc(IFunc):
    id = "calendar_change_func"
    description = """ A transformation adapter.
    Convert years from Ethiopian calendar to Gregorian (standard) calendar.
    """
    inputs = {"graph1": ArgType.Graph(None), "field": ArgType.String}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, graph1: Graph, field: str):
        self.graph = graph1
        self.field = field

        self.converter = EthiopianDateConverter()

    def exec(self) -> dict:
        for node in self.graph.nodes:
            if self.field in node.data:
                start_year = EthiopianDateConverter.to_gregorian(int(node.data), 1, 1)
                end_year = EthiopianDateConverter.to_gregorian(int(node.data), 12, 31)
                node.data[self.field] = f"{start_year}/{end_year}"
            return {"graph": self.graph}

        return {"data": self.graph}

    def validate(self) -> bool:
        return True

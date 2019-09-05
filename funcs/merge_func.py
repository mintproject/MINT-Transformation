from drepr import Graph

from dtran import IFunc, ArgType


class MergeFunc(IFunc):
    id = "merge_func"
    inputs = {"graph1": ArgType.Graph(None), "graph2": ArgType.Graph(None)}
    outputs = {"data": ArgType.Graph(None)}

    def __init__(self, graph1: Graph, graph2: Graph):
        self.graph1 = graph1
        self.graph2 = graph2

    def exec(self) -> dict:
        return {"data": Graph(self.graph1.nodes + self.graph2.nodes, self.graph1.edges + self.graph2.edges)}

    def validate(self) -> bool:
        return True

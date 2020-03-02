from drepr import Graph

from dtran import IFunc, ArgType
from dtran.ifunc import IFuncType


class MergeFunc(IFunc):
    id = "merge_func"
    description = ''' A transformation adapter.
    Merges two graphs into one.
    '''
    inputs = {"data1": ArgType.DataSet, "data2": ArgType.DataSet}
    outputs = {"data": ArgType.DataSet}
    friendly_name: str = "Merge Two Graphs Into One"
    func_type = IFuncType.INTERMEDIATE

    def __init__(self, graph1: Graph, graph2: Graph):
        self.graph1 = graph1
        self.graph2 = graph2

    def exec(self) -> dict:
        return {"data": Graph(self.graph1.nodes + self.graph2.nodes, self.graph1.edges + self.graph2.edges)}

    def validate(self) -> bool:
        return True

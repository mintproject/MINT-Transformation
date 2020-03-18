from typing import Union, List, Dict, Iterable, Callable

from drepr import DRepr
from drepr.models import SemanticModel
from drepr.outputs import ArrayBackend
from drepr.outputs.array_backend.lst_array_class import LstArrayClass
from drepr.outputs.base_lst_output_class import BaseLstOutputClass
from drepr.outputs.base_output_class import BaseOutputClass
from drepr.outputs.base_output_sm import BaseOutputSM
from drepr.outputs.base_record import BaseRecord
from drepr.outputs.graph_backend.lst_graph_class import LstGraphClass
from drepr.outputs.record_id import RecordID


class ShardedClassID(str):
    def __new__(cls, idx: int, class_id: str):
        return super().__new__(cls, class_id)

    def __init__(self, idx: int, class_id: str):
        self.idx = idx


class ShardedBackend(BaseOutputSM):
    def __init__(self, n_chunks: int):
        # list of datasets in the reverse order
        self.n_chunks = n_chunks
        self.datasets: List[BaseOutputSM] = [None] * n_chunks
        self.count = 0

    @classmethod
    def from_drepr(cls, ds_model: Union[DRepr, str], resources: Union[str, Dict[str, str]], inject_class_id: Callable[[str], str] = None) -> BaseOutputSM:
        raise NotImplementedError("This method should never be called")

    def add(self, dataset):
        self.datasets[self.n_chunks - self.count - 1] = dataset
        self.count += 1

    def inject_class_id(self, class_id: str) -> ShardedClassID:
        return ShardedClassID(self.count, class_id)

    def iter_classes(self) -> Iterable[BaseOutputClass]:
        return (c for d in reversed(self.datasets) for c in d.iter_classes())

    def get_record_by_id(self, rid: RecordID) -> BaseRecord:
        return self.datasets[rid.class_id.idx].get_record_by_id(rid)

    def c(self, class_uri: str) -> BaseLstOutputClass:
        if isinstance(self.datasets[0], ArrayBackend) or \
                (isinstance(self.datasets[0], LazyLoadBackend) and self.datasets[0].backend == ArrayBackend):
            return LstArrayClass([c for d in reversed(self.datasets) for c in d.c(class_uri).classes])
        else:
            return LstGraphClass([c for d in reversed(self.datasets) for c in d.c(class_uri).classes])

    def cid(self, class_id: str) -> BaseOutputClass:
        return self.datasets[class_id.idx].cid(class_id)

    def get_sm(self) -> SemanticModel:
        return self.datasets[0].get_sm()

    def drain(self):
        """
        Iterate and remove the dataset out of the list. After this function, the list of datasets should be empty
        """
        for i in range(len(self.datasets)):
            yield self.datasets.pop()


class LazyLoadBackend(BaseOutputSM):
    def __init__(self, backend: BaseOutputSM, drepr: Union[DRepr, str], load_fn: Callable[[], str],
                 del_fn: Callable[[], None], inject_class_id: Callable[[str], str] = None):
        # function that loads the dataset
        self.backend = backend
        self.drepr = drepr
        self.load_fn = load_fn
        self.del_fn = del_fn
        self.inject_class_id = inject_class_id
        self.dataset: BaseOutputSM = None

    def __call__(self, *args, **kwargs) -> BaseOutputSM:
        # magic method that will forward all the calls of this dataset to the correct instance
        # which is loaded by the function self.load_fn
        if self.dataset:
            return self.dataset
        self.dataset = self.backend.from_drepr(self.drepr, self.load_fn(), self.inject_class_id)
        return self.dataset

    def __del__(self):
        self.del_fn()

    @classmethod
    def from_drepr(cls, ds_model: Union[DRepr, str], resources: Union[str, Dict[str, str]], inject_class_id: Callable[[str], str] = None) -> BaseOutputSM:
        raise NotImplementedError("This method should never be called")

    def iter_classes(self) -> Iterable[BaseOutputClass]:
        return self.__call__().iter_classes()

    def get_record_by_id(self, rid: RecordID) -> BaseRecord:
        return self.__call__().get_record_by_id(rid)

    def c(self, class_uri: str) -> BaseLstOutputClass:
        return self.__call__().c(class_uri)

    def cid(self, class_id: str) -> BaseOutputClass:
        return self.__call__().cid(class_id)

    def get_sm(self) -> SemanticModel:
        return self.__call__().get_sm()


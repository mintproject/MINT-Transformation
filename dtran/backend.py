from typing import Union, List, Dict, Iterable

from drepr import DRepr
from drepr.models import SemanticModel
from drepr.outputs.base_lst_output_class import BaseLstOutputClass
from drepr.outputs.base_output_class import BaseOutputClass
from drepr.outputs.base_output_sm import BaseOutputSM
from drepr.outputs.base_record import BaseRecord
from drepr.outputs.record_id import RecordID


class ShardedClassID(str):
    def __new__(cls, index: int, class_id: str):
        return super().__new__(cls, class_id)

    def __init__(self, index: int, class_id: str):
        super().__init__()
        self.index = index
        self.class_id = class_id


class SharedBackend(BaseOutputSM):
    def __init__(self, datasets: List[BaseOutputSM]):
        # list of datasets
        self.datasets = datasets
        """
        for i, dataset in enumerate(self.datasets):
            for output_class in dataset.iter_classes():
                output_class.id = ShardedClassID(i, output_class.id)
        """

    @classmethod
    def from_drepr(
        cls, ds_model: Union[DRepr, str], resources: Union[str, Dict[str, str]]
    ) -> BaseOutputSM:
        raise NotImplementedError("This method should never be called")

    def iter_classes(self) -> Iterable[BaseOutputClass]:
        pass

    def get_record_by_id(self, rid: RecordID) -> BaseRecord:
        pass
        """
        return self.datasets[rid.class_id.index].get_record_by_id(rid)
        """

    def c(self, class_uri: str) -> BaseLstOutputClass:
        pass

    def cid(self, class_id: str) -> BaseOutputClass:
        pass

    def _get_sm(self) -> SemanticModel:
        pass

    def drain(self):
        """
        Iterate and remove the dataset out of the list. After this function, the list of datasets should be empty
        """
        for i in range(len(self.datasets)):
            yield self.datasets.pop()

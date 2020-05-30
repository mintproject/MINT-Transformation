from typing import *
from inspect import signature

from dtran.ifunc import IFunc
from dtran.metadata import Metadata
from funcs import CroppingTransFunc


class CroppingTransWrapper(IFunc):
    func_cls = CroppingTransFunc
    id = CroppingTransFunc.id
    description = CroppingTransFunc.description
    inputs = CroppingTransFunc.inputs
    outputs = CroppingTransFunc.outputs
    func_type = CroppingTransFunc.func_type
    friendly_name = CroppingTransFunc.friendly_name
    example = CroppingTransFunc.example

    def __init__(self, **kwargs):
        try:
            signature(CroppingTransWrapper.func_cls.__init__).bind(CroppingTransWrapper.func_cls, **kwargs)
        except TypeError:
            print(f"Cannot initialize cls: {CroppingTransWrapper.func_cls}")
            raise
        self.func_args = kwargs

    async def exec(self) -> Union[dict, Generator[dict, None, None], AsyncGenerator[dict, None]]:
        func_args = self.func_args.copy()
        shapes = []
        if 'shape' not in func_args:
            shapes.append(None)
        else:
            async for shape in func_args['shape']:
                shapes.append(shape)
        variables = []
        if isinstance(func_args['variable_name'], str):
            variables.append(func_args['variable_name'])
        else:
            async for variable in func_args['variable_name']:
                variables.append(variable)
        async for dataset in func_args['dataset']:
            for shape in shapes:
                for variable in variables:
                    func = self.func_cls(**{**func_args, 'shape': shape, 'variable_name': variable, 'dataset': dataset})
                    func.get_preference = self.get_preference
                    try:
                        yield func.exec()
                    except AssertionError as e:
                        print(e)

    def validate(self) -> bool:
        return True

    def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
        return metadata

#!/usr/bin/python
# -*- coding: utf-8 -*-
from collections import Counter
from pathlib import Path
from typing import *

from inspect import isasyncgenfunction, isgeneratorfunction, iscoroutinefunction, signature
from asyncio import Event, gather, get_event_loop, sleep
from marshmallow import Schema, fields, ValidationError
from networkx import DiGraph, lexicographical_topological_sort, bfs_edges, NetworkXUnfeasible

from dtran.ifunc import IFunc
from dtran.metadata import Metadata
from dtran.wireio import WiredIOArg


class Pipeline(object):
    def __init__(self, func_classes: List[Type[IFunc]], wired: List[any] = None):
        """
        :param func_classes:
        :param wired: input, output
        """
        # map from function id to a tuple (idx of function, order of function (start from 1)).
        self.id2order = {}
        # map from idx of function to its order
        idx2order = {}
        # map from tuple (id, order) of function to its dataset preference
        self.preferences = {}

        for i, func_cls in enumerate(func_classes):
            if func_cls.id not in self.id2order:
                self.id2order[func_cls.id] = []
            self.id2order[func_cls.id].append((i, len(self.id2order[func_cls.id]) + 1))
            idx2order[i] = len(self.id2order[func_cls.id])
            self.preferences[(func_cls.id, idx2order[i])] = {}

        wired = wired or []
        # mapping of wired from input to output
        self.wired = {}
        # inverse mapping of wired from output to all inputs
        self.inv_wired = {}
        # applying topological sort on func_classes to determine execution order based on wiring
        graph = DiGraph()
        graph.add_nodes_from(range(len(func_classes)))
        # mapping preferences of argtype "dataset" to determine backend for "dataset" outputs
        preference_roots, preference_graph = [], DiGraph()
        for i, o in wired:
            if i[1] is None:
                i[1] = self.get_func_order(i[0])
            if o[1] is None:
                o[1] = self.get_func_order(o[0])

            input_arg = func_classes[self.id2order[i[0]][i[1] - 1][0]].inputs[i[2]]
            output_arg = func_classes[self.id2order[o[0]][o[1] - 1][0]].outputs[o[2]]
            if input_arg != output_arg:
                raise ValidationError(
                    f"Incompatible ArgType while wiring {WiredIOArg.get_arg_name(i[0], i[1], i[2])} to {WiredIOArg.get_arg_name(o[0], o[1], o[2])}")
            input_gname = (i[0], i[1], i[2])
            output_gname = (o[0], o[1], o[2])
            self.wired[input_gname] = output_gname
            if output_gname not in self.inv_wired:
                self.inv_wired[output_gname] = []
            self.inv_wired[output_gname].append(input_gname)
            graph.add_edge(self.id2order[o[0]][o[1] - 1][0], self.id2order[i[0]][i[1] - 1][0])

            if output_arg.id == 'dataset':
                self.preferences[(o[0], o[1])][o[2]] = None
                node = (o[0], o[1], 'o', o[2])
                # if input_ref of "dataset" output is None, we take it as a new "dataset"
                if output_arg.input_ref is None:
                    preference_roots.append(node)
                elif output_arg.input_ref not in func_classes[self.id2order[o[0]][o[1] - 1][0]].inputs:
                    raise ValidationError(
                        f"Invalid value for input_ref {output_arg.input_ref} of {output_gname} output dataset")
                elif func_classes[self.id2order[o[0]][o[1] - 1][0]].inputs[output_arg.input_ref] != output_arg:
                    raise ValidationError(
                        f"Invalid ArgType for input_ref {output_arg.input_ref} of {output_gname} output dataset")
                else:
                    # adding dummy "internal" edges within the same adapter to link "dataset" output to its input_ref
                    preference_graph.add_edge((o[0], o[1], 'i', output_arg.input_ref), node, preference='n/a')
                preference_graph.add_edge(node, (i[0], i[1], 'i', i[2]), preference=input_arg.preference)

        self.func_classes = []
        self.idx2order = {}
        try:
            # reordering func_classes in topologically sorted order for execution
            for i in lexicographical_topological_sort(graph):
                self.func_classes.append(func_classes[i])
                # changing idx of functions to map to their new order
                self.idx2order[len(self.func_classes) - 1] = idx2order[i]

        except NetworkXUnfeasible:
            raise ValidationError("Pipeline is not a DAG")

        self.schema = {}
        for i, func_cls in enumerate(self.func_classes):
            for argname in func_cls.inputs:
                input_gname = (func_cls.id, self.idx2order[i], argname)
                if input_gname in self.wired:
                    continue
                argtype = func_cls.inputs[argname]
                self.schema[WiredIOArg.get_arg_name(*input_gname)] = fields.Raw(required=not argtype.optional, validate=argtype.is_valid,
                                                                                error_messages={'validator_failed': f"Invalid Argument type. Expected {argtype.id}"})
        self.schema = Schema.from_dict(self.schema)

        # setting preferences for new "dataset" outputs
        for root in preference_roots:
            counter = Counter()
            # traversing subgraph from every new "dataset" as root and counting preferences
            for edge in bfs_edges(preference_graph, root):
                counter[preference_graph[edge[0]][edge[1]]['preference']] += 1
            preference = None
            if counter['graph'] > counter['array']:
                preference = 'graph'
            elif counter['array'] > counter['graph']:
                preference = 'array'
            self.preferences[(root[0], root[1])][root[3]] = preference

    def exec(self, inputs: dict) -> None:
        inputs_copy = {}
        for arg in inputs:
            if isinstance(arg, WiredIOArg):
                if arg.func_idx is None:
                    func_idx = self.get_func_order(arg.func_id)
                else:
                    func_idx = arg.func_idx
                inputs_copy[WiredIOArg.get_arg_name(arg.func_id, func_idx, arg.name)] = inputs[arg]
            else:
                inputs_copy[arg] = inputs[arg]
        inputs = inputs_copy
        self.validate(inputs)

        # asyncio events to notify consumer of an input that it is ready
        self.input_ready_events = {}
        # asyncio events to notify the producer of an output that a particular wired input has been consumed
        self.input_received_events = {}
        # set of inputs whose consumer is waiting for it to be ready
        self.waiting_ready_inputs = set()
        # set of inputs whose producer is waiting for it to be consumed
        self.waiting_received_inputs = set()
        # set of tasks which are finished, so that any of their consumers/producers can stop
        self.finished_tasks = set()
        self.output = {}
        # list of async tasks, one for each adapter
        tasks = []
        for i, func_cls in enumerate(self.func_classes):
            func_args = {}
            for argname in func_cls.inputs.keys():
                input_gname = (func_cls.id, self.idx2order[i], argname)
                if input_gname in self.wired:
                    # wired has higher priority
                    self.input_ready_events[input_gname] = Event()
                    self.input_received_events[input_gname] = Event()
                    # passing an async generator (or stream) for a wired input
                    func_args[argname] = self.wait_for_input(input_gname)
                else:
                    try:
                        func_args[argname] = inputs[WiredIOArg.get_arg_name(*input_gname)]
                    except KeyError as e:
                        if func_cls.inputs[argname].optional:
                            continue
                        raise e

            tasks.append(self.create_task(i, func_args))
        # run all tasks concurrently in asyncio event loop
        get_event_loop().run_until_complete(gather(*tasks, self.detect_deadlock()))

    async def create_task(self, i: int, func_args: dict):
        func_cls = self.func_classes[i]
        if not isasyncgenfunction(func_cls.exec):
            # use a wrapper for regular or generator adapter
            func_cls = default_wrapper(func_cls, {
                argname for argname in func_cls.inputs.keys()
                if (func_cls.id, self.idx2order[i], argname) in self.wired.keys()
            })
        try:
            func = func_cls(**func_args)
        except TypeError:
            print(f"Cannot initialize cls: {func_cls}")
            raise
        func.set_preferences(self.preferences[(func_cls.id, self.idx2order[i])])
        # list of inputs which have been successfully wired
        wired = []
        # default wired inputs for the case if it never goes inside the async for loop below
        for argname in func_cls.outputs.keys():
            output_gname = (func_cls.id, self.idx2order[i], argname)
            if output_gname in self.inv_wired:
                for input_gname in self.inv_wired[output_gname]:
                    wired.append(input_gname)

        # looping Async Generator Adapter
        async for result in func.exec():
            wired = []
            for argname in func_cls.outputs.keys():
                output_gname = (func_cls.id, self.idx2order[i], argname)
                try:
                    self.output[output_gname] = result[argname]
                except TypeError:
                    print(
                        f"Error while wiring output of {func_cls} from {argname} to {WiredIOArg.get_arg_name(func_cls.id, self.idx2order[i], argname)}"
                    )
                    raise
                if output_gname in self.inv_wired:
                    for input_gname in self.inv_wired[output_gname]:
                        if input_gname[:2] not in self.finished_tasks:
                            # notifying consumer of an input that it is ready
                            self.input_ready_events[input_gname].set()
                            self.waiting_ready_inputs.discard(input_gname)
                            wired.append(input_gname)

            # waiting for all wired inputs to be consumed
            for input_gname in wired:
                # adding input to the waiting set only if it's not already consumed
                if not self.input_received_events[input_gname].is_set():
                    self.waiting_received_inputs.add(input_gname)
                # waiting for an input to be consumed
                await self.input_received_events[input_gname].wait()
                self.input_received_events[input_gname].clear()

        self.finished_tasks.add((func_cls.id, self.idx2order[i]))
        # notifying all wired inputs that producer has finished
        for input_gname in wired:
            if input_gname[:2] not in self.finished_tasks:
                self.input_ready_events[input_gname].set()
                self.waiting_ready_inputs.discard(input_gname)

        # notifying all producers which are still running that consumer has finished
        for argname in func_cls.inputs.keys():
            input_gname = (func_cls.id, self.idx2order[i], argname)
            if (input_gname in self.wired) and (self.wired[input_gname][:2] not in self.finished_tasks):
                self.input_received_events[input_gname].set()
                self.waiting_received_inputs.discard(input_gname)

    async def wait_for_input(self, input_gname: Tuple[str, int, str]):
        while True:
            # adding input to the waiting set only if it's not already ready
            if not self.input_ready_events[input_gname].is_set():
                self.waiting_ready_inputs.add(input_gname)
            # waiting for an input to be ready
            await self.input_ready_events[input_gname].wait()
            self.input_ready_events[input_gname].clear()
            # break out of the loop if producer has finished
            if self.wired[input_gname][:2] in self.finished_tasks:
                break
            # notifying producer of an input that it has been consumed
            self.input_received_events[input_gname].set()
            self.waiting_received_inputs.discard(input_gname)
            yield self.output[self.wired[input_gname]]

    async def detect_deadlock(self):
        while len(self.finished_tasks) < len(self.func_classes):
            # checking if all running tasks are waiting, the deadlock condition
            if len(self.func_classes) - len(self.finished_tasks) == len(self.waiting_ready_inputs) + len(self.waiting_received_inputs):
                raise RuntimeError("Pipeline went into a deadlock")
            await sleep(0)

    def validate(self, inputs: dict) -> None:
        errors = self.schema().validate(inputs)
        if errors:
            raise ValidationError(errors)

    def get_func_order(self, func_id: str) -> int:
        if len(self.id2order[func_id]) == 0:
            raise ValueError(f"Cannot wire argument to function {func_id} because it doesn't exist")

        if len(self.id2order[func_id]) > 1:
            raise ValueError(
                f"Cannot wire argument to function {func_id} because it is ambiguous (more than one function with same id)"
            )

        return self.id2order[func_id][0][-1]

    def save(self, save_path: Union[str, Path]):
        pass

    @staticmethod
    def load(load_path: Union[str, Path]):
        pass


def default_wrapper(cls: Type[IFunc], inputs: Set[str]) -> Type[IFunc]:
    class DefaultWrapper(IFunc):
        func_cls = cls

        def __init__(self, **kwargs):
            # checking if the kwargs satisfy wrapped adapter's init function definition
            try:
                signature(DefaultWrapper.func_cls.__init__).bind(DefaultWrapper.func_cls, **kwargs)
            except TypeError:
                print(f"Cannot initialize cls: {DefaultWrapper.func_cls}")
                raise
            self.func_args = kwargs

        async def exec(self) -> Union[dict, Generator[dict, None, None], AsyncGenerator[dict, None]]:
            while True:
                func_args = self.func_args.copy()
                # waiting for all wired inputs, and break out of the loop if any one of them has finished
                try:
                    for argname in inputs:
                        func_args[argname] = await self.func_args[argname].__anext__()
                except StopAsyncIteration:
                    break
                func = self.func_cls(**func_args)
                # TODO: correctly handle validate and change_metadata in future
                # correctly handle get_preference for wrapped adapter's instance
                func.get_preference = self.get_preference
                if isgeneratorfunction(func.exec):
                    for result in func.exec():
                        yield result
                elif iscoroutinefunction(func.exec):
                    yield await func.exec()
                else:
                    yield func.exec()
                # if there are no wired inputs, break out to avoid looping infinitely
                if len(inputs) == 0:
                    break

        def validate(self) -> bool:
            return True

        def change_metadata(self, metadata: Optional[Dict[str, Metadata]]) -> Dict[str, Metadata]:
            return metadata

    # setting static properties of DefaultWrapper to proxy wrapped adapter
    for prop in dir(cls):
        if (not prop.startswith("__")) and \
                (prop not in {'exec', 'validate', 'change_metadata', 'preferences', 'get_preference', 'set_preferences'}):
            setattr(DefaultWrapper, prop, getattr(cls, prop))
    return DefaultWrapper

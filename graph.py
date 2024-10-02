import asyncio
import graphlib
import replicate
import uuid


class Node(object):
    def __init__(self):
        self._id = uuid.uuid4()

    async def execute(self, *args):
        pass

    @property
    def id(self):
        return self._id


class PredictionNode(Node):
    def __init__(self, version):
        super().__init__()
        # FIXME not unique enough, good for demo though
        self._id = f"predict on {version}"
        self._version = version

    async def execute(self, *args):
        input = args[0]
        print(f"starting prediction on {self._version} with {input}")
        return await replicate.async_run(self._version, input)


class ValueNode(Node):
    def __init__(self, value):
        super().__init__()
        self._id = value
        self._value = value

    async def execute(self, *args):
        return self._value


class TransformNode(Node):
    def __init__(self, f):
        super().__init__()
        self._f = f

    async def execute(self, *args):
        return await self._f(*args)


class Composite(object):
    def __init__(self):
        self._graph = {}
        self._nodes = {}
        self._output = {}

    def add(self, node: Node, *inputs: Node) -> None:
        dependencies = [node.id for node in inputs]
        self._nodes[node.id] = node
        self._graph[node.id] = dependencies

    async def _visit(self, n: Node, sorter: graphlib.TopologicalSorter, *args):
        self._output[n.id] = await n.execute(*args)
        print(f"node {n} returned {self._output[n.id]}")
        sorter.done(n.id)

    async def execute(self):
        sorter = graphlib.TopologicalSorter(self._graph)
        sorter.prepare()
        while sorter.is_active():
            node_group = sorter.get_ready()

            if not node_group:
                await asyncio.sleep(0.25)
                continue
            tasks = set()
            for node in node_group:
                try:
                    inputs = [self._output[dep] for dep in self._graph[node]]
                except Exception:
                    print(
                        f"no output. outputs {self._output} dependencies {self._graph[node]}"
                    )
                    raise
                task = asyncio.create_task(
                    self._visit(self._nodes[node], sorter, *inputs)
                )
                tasks.add(task)
                task.add_done_callback(tasks.discard)


def predict(version: str) -> PredictionNode:
    return PredictionNode(version)


def value(value) -> ValueNode:
    return ValueNode(value)


def transform(f) -> TransformNode:
    return TransformNode(f)

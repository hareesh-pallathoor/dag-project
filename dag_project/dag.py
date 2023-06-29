import asyncio
import sys
import loguru

from dag_project.logger import configure_logger

class DAGTaskNode:
    def __init__(self, node_id, _function):
        self.node_id = node_id
        self.dependencies = []
        self.function = _function


class DAGTask:
    def __init__(self, project_name="Unnamed DAGTask"):
        self.nodes = {}
        self.complete = []
        self.context = {}
        configure_logger(project_name)

    def add_task(self, node_id, _function, dependencies=None):
        node = DAGTaskNode(node_id, _function)
        self.nodes[node_id] = node

        if dependencies is not None:
            node.dependencies = dependencies

    def run(self, node_id):
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(self._run(node_id))
        else:
            loop.run_until_complete(self._run(node_id))

    async def _run(self, node_id):
        node = self.nodes[node_id]
        if node.dependencies:
            await asyncio.gather(*[self._run(dep) for dep in node.dependencies])

        await self.execute(node)

    async def execute(self, node):
        if node.node_id not in self.complete:
            loguru.logger.info(f"Executing node: {node.node_id}")
            await node.function
            self.complete.append(node.node_id)

    async def trigger_dag(dag):
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(await dag.run(dag))
        else:
            loop.run_until_complete(await dag.run(dag))

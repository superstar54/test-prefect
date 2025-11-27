from node_graph import node
from node_graph.socket_spec import namespace
from typing import Any
from node_graph_engine.engines.prefect import PrefectEngine
from aiida import load_profile
from prefect import flow


@node()
def add(x, y):
    import time
    print("Adding", x, "+", y)
    time.sleep(5)
    return x + y

@node()
def multiply(x, y):
    import time
    print("Multiplying", x, "*", y)
    time.sleep(5)
    return x * y

@node.graph(outputs=namespace(sum=Any, product=Any))
def add_multiply(x=None, y=None):
    out1 = add(x, y)
    out2 = multiply(out1.result, y)
    return {"sum": out1.result, "product": out2.result}



@flow(name="run-ng")
def run_ng():
    ng = add_multiply.build(1, 2)

    engine = PrefectEngine()
    return engine.run(ng)

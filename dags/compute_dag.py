from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import inspect


def parse_graph(raw_graph: dict):
    return {k: eval(v) for k, v in raw_graph.items()}

def find_required_nodes(graph, outputs):
    """Recursively find steps to calculate"""
    required = set()

    def visit(node):
        if node in required:
            return
        required.add(node)
        func = graph.get(node)
        if func:
            arg_names = inspect.getfullargspec(func).args
            for dep in arg_names:
                visit(dep)

    for out in outputs:
        visit(out)

    return required


def validate_dependencies(graph, inputs, outputs):
    required = find_required_nodes(graph, outputs)
    missing = {n for n in required if n not in inputs and n not in graph}
    if missing:
        raise ValueError(f"Cannot compute outputs {outputs}. Missing definitions for: {missing}")
    return required


def compute_node_task(node_name: str, **kwargs):
    ti = kwargs['ti']
    params = kwargs['params']
    inputs = dict(params["inputs"])
    graph = parse_graph(params["graph"])

    if node_name in inputs:
        result = inputs[node_name]
        print(f"[{node_name}] Using passed input: {result}")
        return result

    func = graph.get(node_name)
    if not func:
        raise ValueError(f"No function defined for node '{node_name}'")

    arg_names = inspect.getfullargspec(func).args
    args = {}
    for dep in arg_names:
        if dep in inputs:
            args[dep] = inputs[dep]
        else:
            val = ti.xcom_pull(task_ids=dep)
            if val is None:
                raise ValueError(f"Missing dependency '{dep}' for node '{node_name}'")
            args[dep] = val

    result = func(**args)
    print(f"[{node_name}] Computed value: {result}")
    return result


with DAG(
    dag_id='graph_based_compute_dag_simplified',
    default_args={'start_date': datetime(2023, 1, 1)},
    schedule_interval=None,
    catchup=False,
    params={
        "inputs": {
            #  "A": 5 or taken from graph
        },
        "outputs": ["D"],
        "graph": {
            "A": "lambda: 5",
            "B": "lambda A: A + 10",
            "C": "lambda A: A * 2",
            "D": "lambda A, B, C: A + B + C"
        }
    }
) as dag:

    raw_params = dag.params
    raw_graph = raw_params["graph"]
    inputs = raw_params["inputs"]
    outputs = raw_params["outputs"]

    graph = parse_graph(raw_graph)
    required_nodes = validate_dependencies(graph, inputs, outputs)

    task_dict = {}

    for node in required_nodes:
        task = PythonOperator(
            task_id=node,
            python_callable=compute_node_task,
            provide_context=True,
            op_kwargs={"node_name": node}
        )
        task_dict[node] = task

    for node in required_nodes:
        if node in inputs:
            continue  # no need to compute, taken from input
        func = graph.get(node)
        if func:
            arg_names = inspect.getfullargspec(func).args
            for dep in arg_names:
                if dep in task_dict:
                    task_dict[dep] >> task_dict[node]
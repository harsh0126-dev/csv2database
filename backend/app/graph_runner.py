# graph_runner.py
"""
A very small graph runner that executes nodes sequentially.
This is intentionally simple so it is easy to replace with a real LangGraph
implementation later: the function names and signatures match the node functions.
"""

from typing import Callable, List, Dict

def run_sequential_nodes(nodes: List[Callable], initial_state: Dict) -> Dict:
    """
    nodes: list of callables. Each callable accepts a state dict and returns state dict.
           But the first node may be load_csv_node which expects (file_bytes, filename)
           so we will handle that by wrapping initial_state keys.
    initial_state: dict with keys:
       - 'file_bytes' : bytes
       - 'filename' : str
       - optional 'pk_candidate' : str
    """
    state = {}

    # First node typically expects bytes + filename; support that with explicit call
    first = nodes[0]
    # Call first node with raw file arguments
    state = first(initial_state["file_bytes"], initial_state["filename"])

    # For subsequent nodes, pass the state dict
    for node in nodes[1:]:
        # Some nodes accept (state, ...) while others accept just state
        # We'll call with state and allow nodes to ignore extra args if needed
        state = node(state)

    return state

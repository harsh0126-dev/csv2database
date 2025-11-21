from typing import Callable, List, Dict

def run_sequential_nodes(nodes: List[Callable], initial_state: Dict) -> Dict:
    state = {}

    # First node receives raw bytes
    first = nodes[0]
    state = first(initial_state["file_bytes"], initial_state["filename"])

    # Next nodes receive full state
    for node in nodes[1:]:
        state = node(state)

    return state

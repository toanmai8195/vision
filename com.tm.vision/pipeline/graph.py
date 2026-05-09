from langgraph.graph import StateGraph, END
from pipeline.state import AgentState
from pipeline.nodes.planner import planner_node
from pipeline.nodes.implementer import implementer_node


def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("planner", planner_node)
    graph.add_node("implementer", implementer_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "implementer")
    graph.add_edge("implementer", END)

    return graph.compile()

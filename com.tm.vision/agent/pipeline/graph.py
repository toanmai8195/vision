from langgraph.graph import StateGraph, END
from agent.pipeline.state import AgentState
from agent.pipeline.nodes.planner import planner_node
from agent.pipeline.nodes.implementer import implementer_node


def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("planner", planner_node)
    graph.add_node("implementer", implementer_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "implementer")
    graph.add_edge("implementer", END)

    return graph.compile()

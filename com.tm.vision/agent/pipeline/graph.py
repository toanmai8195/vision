from langgraph.graph import END, StateGraph

from agent.pipeline.nodes.git_ops import git_push_node
from agent.pipeline.nodes.implementer import implementer_node
from agent.pipeline.nodes.planner import planner_node
from agent.pipeline.nodes.reviewer import reviewer_node
from agent.pipeline.state import AgentState


def _route_after_review(state: AgentState) -> str:
    if state["approved"] or state.get("review_iterations", 0) >= 3:
        return "git_push"
    return "implementer"


def build_graph():
    graph = StateGraph(AgentState)

    graph.add_node("planner", planner_node)
    graph.add_node("implementer", implementer_node)
    graph.add_node("reviewer", reviewer_node)
    graph.add_node("git_push", git_push_node)

    graph.set_entry_point("planner")
    graph.add_edge("planner", "implementer")
    graph.add_edge("implementer", "reviewer")
    graph.add_conditional_edges(
        "reviewer",
        _route_after_review,
        {"git_push": "git_push", "implementer": "implementer"},
    )
    graph.add_edge("git_push", END)

    return graph.compile()

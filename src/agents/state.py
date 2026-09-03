from typing import List, Optional

from langgraph.graph import MessagesState


class SupervisorState(MessagesState):
    """Shared graph state: chat messages plus routing and worker outputs."""

    next: Optional[str]
    completed_workers: List[str]
    campaign_plan: Optional[dict]
    compliance_result: Optional[dict]
    qa_result: Optional[dict]

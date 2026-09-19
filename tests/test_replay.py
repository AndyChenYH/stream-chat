"""A committed V1 history catches incompatible future workflow changes in CI."""
from pathlib import Path
import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from agent_sdk.workflows import AgentWorkflow


@pytest.mark.asyncio
async def test_committed_v1_history_replays_without_external_io():
    path=Path(__file__).parent/'fixtures'/'agent-v1.history.json'
    await Replayer(workflows=[AgentWorkflow]).replay_workflow(
        WorkflowHistory.from_json('synthetic-agent-v1',path.read_text()))

"""Tool definitions are separate from agents; sandbox handlers are in backend/agent.py."""
from pydantic import BaseModel, ConfigDict, Field
from agent_sdk import Tool, ToolRegistry


class Arguments(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)


class Terminal(Arguments):
    command: str = Field(min_length=1, max_length=16000, pattern=r'\S')


class Python(Arguments):
    code: str = Field(min_length=1, max_length=16000, pattern=r'\S')


class PublishFile(Arguments):
    path: str = Field(min_length=1, max_length=1000, pattern=r'^/home/user/')


TOOLS = ToolRegistry([
    Tool('terminal', 'Run a Linux shell command in /home/user. No network. Maximum 30 seconds.', Terminal),
    Tool('python', 'Run Python with pandas, numpy and matplotlib. Variables persist for this run only. '
         'Print answers; plt.show() saves plots. Maximum 30 seconds.', Python),
    Tool('publish_file', 'Save a file before the sandbox is destroyed. Absolute /home/user/ path; maximum 2 MB.', PublishFile),
])

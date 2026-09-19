import json
import pytest
from pydantic import BaseModel, ConfigDict, Field
from agent_sdk import Agent, Limits
from agent_sdk.definitions import validate_tool, validate_output
from backend.tools import TOOLS


def call(name='python', args=None):
    return {'id':'call-1','type':'function','function':{'name':name,'arguments':json.dumps(args or {'code':'print(42)'})}}


def test_agent_compilation_allowlist_and_strict_arguments():
    config = Agent('calculator','Be precise.',tools=('python',)).compile(TOOLS)
    assert [t['function']['name'] for t in config['tools']] == ['python']
    assert validate_tool(call(),config['tools']) == ('python',{'code':'print(42)'})
    for invalid in [call('terminal',{'command':'pwd'}), call(args={'code':123}),call(args={'code':'x','unexpected':True}),call(args={'code':'  '})]:
        with pytest.raises(Exception):
            validate_tool(invalid,config['tools'])
    with pytest.raises(ValueError,match='Unknown tool'):
        Agent('bad','prompt',tools=('unknown',)).compile(TOOLS)
    with pytest.raises(ValueError,match='Duplicate'):
        Agent('bad','prompt',tools=('python','python')).compile(TOOLS)
    with pytest.raises(ValueError):
        Limits(max_model_calls=1000)


def test_structured_final_result_must_match_schema():
    class Result(BaseModel):
        model_config = ConfigDict(extra='forbid')
        answer: int = Field(ge=0)
    config = Agent('json','Answer.',output_type=Result).compile(TOOLS)
    assert validate_output('{"answer":42}',config['output_schema']) == {'answer':42}
    for invalid in ['```json\n{"answer":42}\n```','{"answer":"42"}','{"answer":-1}','{"answer":42,"extra":0}']:
        with pytest.raises(Exception):
            validate_output(invalid,config['output_schema'])

"""Versioned agent configuration, tool allowlists and strict JSON validation."""
from dataclasses import dataclass, field
import json
import re
from typing import Type

from jsonschema import Draft202012Validator
from pydantic import BaseModel


@dataclass(frozen=True)
class Limits:
    max_model_calls: int = 8
    max_tool_calls: int = 6
    max_validation_retries: int = 1
    max_model_retries: int = 1
    max_output_tokens: int = 1024
    run_timeout_s: int = 720

    def __post_init__(self):
        for key, cap in [('max_model_calls', 12), ('max_tool_calls', 6),
                         ('max_validation_retries', 2), ('max_model_retries', 2),
                         ('max_output_tokens', 1024), ('run_timeout_s', 720)]:
            value = getattr(self, key)
            minimum = 1 if key in ('max_model_calls', 'max_output_tokens', 'run_timeout_s') else 0
            if type(value) is not int or not minimum <= value <= cap:
                raise ValueError(f'{key} must be between {minimum} and {cap}')


@dataclass(frozen=True)
class Tool:
    name: str
    description: str
    arguments: Type[BaseModel]
    timeout_s: int = 30

    def schema(self):
        return {'type': 'function', 'function': {'name': self.name,
                'description': self.description, 'parameters': self.arguments.model_json_schema()}}


class ToolRegistry:
    def __init__(self, tools):
        self.tools = {}
        for tool in tools:
            if not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', tool.name) or tool.name in self.tools:
                raise ValueError('Invalid or duplicate tool name')
            if not 1 <= tool.timeout_s <= 30:
                raise ValueError('Tool timeout must be between 1 and 30 seconds')
            self.tools[tool.name] = tool

    def schemas(self, names):
        if len(set(names)) != len(names):
            raise ValueError('Duplicate agent tool')
        try:
            return [self.tools[name].schema() for name in names]
        except KeyError as exc:
            raise ValueError(f'Unknown tool: {exc.args[0]}') from None

    def validate(self, call, allowed):
        name, raw = call_envelope(call)
        if name not in allowed or name not in self.tools:
            raise ValueError('Tool is not permitted for this agent')
        return name, self.tools[name].arguments.model_validate_json(raw, strict=True).model_dump()


@dataclass(frozen=True)
class Agent:
    name: str
    system_prompt: str
    tools: tuple[str, ...] = ()
    version: int = 1
    limits: Limits = field(default_factory=Limits)
    output_type: Type[BaseModel] | None = None

    def compile(self, registry):
        if not self.system_prompt.strip() or len(self.system_prompt) > 16000 or self.version < 1:
            raise ValueError('A bounded system prompt and positive version are required')
        output = self.output_type.model_json_schema() if self.output_type else None
        if output:
            Draft202012Validator.check_schema(output)
        # Only JSON data enters Temporal. Never serialize credentials, handlers or Python types.
        prompt = self.system_prompt
        if output:
            prompt += '\nReturn your final answer as JSON matching this schema, without Markdown fences:\n' + json.dumps(output)
        return {'name': self.name, 'version': self.version, 'system_prompt': prompt,
                'tools': registry.schemas(self.tools), 'limits': vars(self.limits), 'output_schema': output}


def call_envelope(call):
    if not isinstance(call, dict) or call.get('type') != 'function':
        raise ValueError('Invalid tool call envelope')
    if not isinstance(call.get('id'), str) or not 1 <= len(call['id']) <= 128:
        raise ValueError('Invalid tool call ID')
    fn = call.get('function', {})
    if not isinstance(fn, dict) or not isinstance(fn.get('name'), str):
        raise ValueError('Missing tool name')
    raw = fn.get('arguments')
    if not isinstance(raw, str) or len(raw) > 16000:
        raise ValueError('Invalid tool arguments envelope')
    return fn['name'], raw


def validate_tool(call, schemas):
    name, raw = call_envelope(call)
    schema = next((t['function']['parameters'] for t in schemas if t['function']['name'] == name), None)
    if schema is None:
        raise ValueError('Tool is not permitted for this agent')
    args = json.loads(raw)
    Draft202012Validator(schema).validate(args)
    return name, args


def validate_output(text, schema):
    if schema is None:
        return None
    value = json.loads(text)
    Draft202012Validator(schema).validate(value)
    return value

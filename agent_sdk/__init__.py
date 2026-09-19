"""Small, provider-independent agent definitions. Execution lives in workflows.py."""
__all__ = ['Agent', 'Limits', 'Tool', 'ToolRegistry']


def __getattr__(name):
    # Keep package import free of Pydantic/jsonschema side effects in Temporal's sandbox.
    if name in __all__:
        from . import definitions
        return getattr(definitions, name)
    raise AttributeError(name)

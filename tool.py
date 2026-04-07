import copy
import os
import sys
from typing import Any, Dict, List, Optional

def app_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.abspath(__file__))


def deep_copy(value: Any) -> Any:
    return copy.deepcopy(value)


def compact(value: Any) -> Any:
    """Remove None / empty strings / empty dicts / empty lists, but keep False and 0."""
    if isinstance(value, dict):
        result = {}
        for key, sub in value.items():
            compacted = compact(sub)
            if compacted is None:
                continue
            if compacted == "":
                continue
            if compacted == {}:
                continue
            if compacted == []:
                continue
            result[key] = compacted
        return result
    if isinstance(value, list):
        result = []
        for item in value:
            compacted = compact(item)
            if compacted is None:
                continue
            if compacted == "":
                continue
            if compacted == {}:
                continue
            if compacted == []:
                continue
            result.append(compacted)
        return result
    return value

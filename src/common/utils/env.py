# src/common/utils/env.py
import os
from typing import TypeVar, Callable, Any

T = TypeVar("T")


def get_env(
    key: str,
    default: T,
    cast: Callable[[str], Any] | None = None,
) -> T:
    """
    Helper nhỏ để đọc env với default & cast type.
    """
    value = os.getenv(key)
    if value is None:
        return default
    if cast:
        return cast(value)  # type: ignore[return-value]
    return value  # type: ignore[return-value]

"""Helpers shared by the notebook-facing sessions (`SdiSession`, `DdsSession`).

- :func:`friendly_errors` turns every failure a notebook user can act on into
  one session error, so a magic prints a short line instead of a traceback.
- :func:`check_arg_types` reports a wrongly typed argument by name and position.
- :func:`find_dotenv` / :func:`read_dotenv_value` read one setting from the
  nearest ``.env`` without touching ``os.environ``.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from functools import wraps
from pathlib import Path
from typing import Any, TypeVar

DOTENV_NAME = ".env"

_F = TypeVar("_F", bound=Callable[..., Any])


def plain_param(name: str, param: inspect.Parameter) -> str:
    """One parameter as a notebook user reads it: ``name``, ``name=default``,
    or ``name...`` for one that takes any number of values (``*name``)."""
    if param.kind is inspect.Parameter.VAR_POSITIONAL:
        return f"{name}..."
    return name if param.default is inspect.Parameter.empty else f"{name}={param.default!r}"


def call_usage(method: Callable[..., Any]) -> str:
    """``name(a, b=None, ...)`` — how to call ``method``, without ``self`` or type hints."""
    params = [
        plain_param(name, param)
        for name, param in inspect.signature(method).parameters.items()
        if name != "self"
    ]
    return f"{method.__name__}({', '.join(params)})"


def friendly_errors(
    error: type[RuntimeError], expected: tuple[type[BaseException], ...]
) -> Callable[[_F], _F]:
    """Re-raise every ``expected`` failure of the decorated method as ``error``.

    A call that doesn't fit the method's signature (an unknown keyword, an
    argument given twice, too many arguments) is reported the same way, with
    the correct form of the call. Anything else is a bug and keeps its
    traceback.
    """

    def decorate(method: _F) -> _F:
        signature = inspect.signature(method)

        @wraps(method)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                signature.bind(*args, **kwargs)
            except TypeError as exc:
                raise error(f"{method.__name__}: {exc}\nusage: {call_usage(method)}") from None
            try:
                return method(*args, **kwargs)
            except error:
                raise
            except expected as exc:
                raise error(str(exc) or type(exc).__name__) from exc

        return wrapper  # type: ignore[return-value]

    return decorate


def check_arg_types(func: str, *specs: tuple[str, object, type | tuple[type, ...], bool]) -> None:
    """Raise ``TypeError`` unless every argument has exactly its declared type.

    ``specs`` are ``(name, value, expected, optional)`` in the function's
    parameter order, so the message can name the position too — the argument
    most likely to be wrong is one passed positionally into the wrong slot.
    Strict: ``bool`` parameters take only ``True`` / ``False`` (not ``0`` /
    ``1``), and a ``str`` parameter takes no other type. ``optional`` allows
    ``None``.
    """
    for position, (name, value, expected, optional) in enumerate(specs, start=1):
        if value is None and optional:
            continue
        kinds = expected if isinstance(expected, tuple) else (expected,)
        # bool is a subclass of int: only a bool parameter may take a bool.
        if any(
            isinstance(value, kind) and (kind is bool or not isinstance(value, bool))
            for kind in kinds
        ):
            continue
        wanted = " or ".join(kind.__name__ for kind in kinds) + (" or None" if optional else "")
        raise TypeError(
            f"{func}: argument {position} ({name}) must be {wanted}, not {type(value).__name__}"
        )


def find_dotenv(start: str | Path | None = None) -> Path | None:
    """The first ``.env`` in ``start`` (default: the working directory) or a parent."""
    here = Path.cwd() if start is None else Path(start)
    for folder in (here, *here.resolve().parents):
        candidate = folder / DOTENV_NAME
        if candidate.is_file():
            return candidate
    return None


def read_dotenv_value(path: Path, key: str) -> str | None:
    """``key``'s value in the ``KEY=VALUE`` file ``path``, or ``None``.

    Blank lines, ``#`` comments and a leading ``export`` are allowed; one pair
    of surrounding quotes is stripped. An empty value counts as unset.
    """
    found: str | None = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ").strip()
        name, sep, value = line.partition("=")
        if not sep or name.strip() != key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        found = value or None  # the last assignment wins, as in a shell
    return found

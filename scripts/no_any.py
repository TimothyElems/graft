"""Fail if `Any` appears anywhere in the SDK's public API.

    python scripts/no_any.py            # exit 1 and list every hit

Public API = everything reachable from graft.__all__: functions, classes, their public
methods and properties, model fields, and any graft class those annotations mention
(so Graft.branches leads to Branches.create). Members inherited from outside the
graft package (pydantic.BaseModel, Exception) are not ours and are skipped.
"""
from __future__ import annotations

import inspect
import sys
import typing
from collections.abc import Callable, Iterator

import graft

DUNDERS_WE_OWN = {"__init__", "__call__", "__iter__", "__aiter__", "__enter__", "__exit__", "__aenter__", "__aexit__"}


def has_any(tp: object) -> bool:
    if tp is typing.Any:
        return True
    return any(has_any(a) for a in typing.get_args(tp))


def graft_classes_in(tp: object) -> Iterator[type]:
    if isinstance(tp, type) and tp.__module__.startswith("graft"):
        yield tp
    for a in typing.get_args(tp):
        yield from graft_classes_in(a)
    origin = typing.get_origin(tp)
    if isinstance(origin, type) and origin.__module__.startswith("graft"):
        yield origin


def hints(obj: object) -> dict[str, object]:
    try:
        return typing.get_type_hints(obj)
    except Exception as e:  # an annotation that cannot even be resolved is a failure too
        raise SystemExit(f"cannot resolve annotations of {obj!r}: {e}") from e


def check_callable(where: str, fn: Callable[..., object], found: list[str], todo: list[type]) -> None:
    for name, tp in hints(fn).items():
        if has_any(tp):
            found.append(f"{where}: {'return' if name == 'return' else 'parameter ' + name} is {tp}")
        todo.extend(graft_classes_in(tp))


def check_class(cls: type, found: list[str], todo: list[type]) -> None:
    for owner in cls.__mro__:
        if not owner.__module__.startswith("graft"):
            continue
        own = owner.__dict__.get("__annotations__", {})
        resolved = hints(owner)
        for name in own:
            if name.startswith("_"):
                continue
            tp = resolved.get(name)
            if has_any(tp):
                found.append(f"{owner.__module__}.{owner.__qualname__}.{name}: attribute is {tp}")
            todo.extend(graft_classes_in(tp))
        for name, member in owner.__dict__.items():
            if name.startswith("_") and name not in DUNDERS_WE_OWN:
                continue
            fn = member.fget if isinstance(member, property) else getattr(member, "__func__", member)
            if inspect.isfunction(fn):
                check_callable(f"{owner.__module__}.{owner.__qualname__}.{name}", fn, found, todo)


def main() -> int:
    found: list[str] = []
    todo: list[type] = []
    for name in graft.__all__:
        obj = getattr(graft, name)
        if inspect.isclass(obj):
            todo.append(obj)
        elif inspect.isfunction(obj):
            check_callable(f"graft.{name}", obj, found, todo)
    seen: set[type] = set()
    while todo:
        cls = todo.pop()
        if cls in seen:
            continue
        seen.add(cls)
        check_class(cls, found, todo)
    for line in sorted(set(found)):
        print(line)
    print(f"checked {len(seen)} public classes: {len(set(found))} uses of Any", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Error types and Result pattern placeholders (Phase 1 UI scaffold)."""

from __future__ import annotations
from dataclasses import dataclass
from enum import Enum, auto
from typing import Generic, TypeVar, Union, Optional

T = TypeVar("T")
E = TypeVar("E")

class ErrorKind(Enum):
    UI = auto()
    GENERIC = auto()

@dataclass(frozen=True)
class AppError:
    kind: ErrorKind
    message: str
    source: Optional[str] = None

    def __str__(self) -> str:
        base = f"{self.kind.name}: {self.message}"
        return f"{base} (source: {self.source})" if self.source else base

@dataclass(frozen=True)
class Ok(Generic[T]):
    value: T

@dataclass(frozen=True)
class Err(Generic[E]):
    error: E

Result = Union[Ok[T], Err[E]]

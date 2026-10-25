"""Pure validator for Airflow schedule strings (no croniter dependency)."""

from __future__ import annotations

PRESETS = frozenset({"@once", "@hourly", "@daily", "@weekly", "@monthly", "@quarterly", "@yearly"})
_MONTHS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")
_DAYS = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
# (low, high, names) for minute, hour, day of month, month, day of week
_FIELDS: tuple[tuple[int, int, tuple[str, ...]], ...] = (
    (0, 59, ()),
    (0, 23, ()),
    (1, 31, ()),
    (1, 12, _MONTHS),
    (0, 7, _DAYS),
)


def _number(token: str) -> bool:
    return token.isascii() and token.isdigit()


def _value(token: str, low: int, high: int, names: tuple[str, ...]) -> int | None:
    if token.lower() in names:
        return names.index(token.lower()) + (low if names is _MONTHS else 0)
    if _number(token) and low <= int(token) <= high:
        return int(token)
    return None


def _field_ok(field: str, low: int, high: int, names: tuple[str, ...]) -> bool:
    for item in field.split(","):
        base, has_step, step = item.partition("/")
        if has_step and not (_number(step) and int(step) >= 1):
            return False
        if base == "*":
            continue
        start, has_range, end = base.partition("-")
        first = _value(start, low, high, names)
        last = _value(end, low, high, names) if has_range else first
        if first is None or last is None or first > last:
            return False
    return True


def is_valid_schedule(schedule: str | None) -> bool:
    """True for None, a preset, or five cron fields with ranges, steps, lists and names."""
    if schedule is None or schedule in PRESETS:
        return True
    parts = schedule.split()
    return len(parts) == 5 and all(
        _field_ok(p, lo, hi, names) for p, (lo, hi, names) in zip(parts, _FIELDS, strict=True)
    )

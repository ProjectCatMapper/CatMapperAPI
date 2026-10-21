"""Metadata-backed validation shared by USES writes and derived events."""
import json
import math
from .utils import getQuery


def get_allowed_event_types(driver):
    """Read the authoritative vocabulary each time; missing metadata is an error."""
    rows = getQuery("""
        MATCH (p:PROPERTY {CMName: 'eventType', type: 'relationship'})
        RETURN p.allowedValues AS allowedValues
    """, driver)
    if len(rows) != 1:
        raise ValueError("Expected one eventType PROPERTY metadata node")
    values = rows[0].get("allowedValues")
    if (not isinstance(values, list) or not values
            or any(not isinstance(v, str) or not v or v != v.strip() for v in values)
            or len(set(values)) != len(values)):
        raise ValueError("eventType PROPERTY.allowedValues must be a nonempty list of unique strings")
    return tuple(values)


def is_empty_event_value(value):
    return value is None or (isinstance(value, str) and value == "") or (
        isinstance(value, float) and math.isnan(value)
    )


def validate_event_type(value, allowed_values, *, normalize_legacy=False):
    if is_empty_event_value(value):
        return
    candidate = value
    if normalize_legacy and isinstance(value, str):
        matches = [allowed for allowed in allowed_values
                   if allowed.casefold() == value.strip().casefold()]
        if len(matches) == 1:
            candidate = matches[0]
    if not isinstance(candidate, str) or candidate not in allowed_values:
        raise ValueError(f"Invalid eventType: {value!r}. Allowed values: {', '.join(allowed_values)}")
    return candidate


def parent_context_entries(value):
    """Decode scalar/list JSON and mappings without swallowing malformed entries."""
    if is_empty_event_value(value):
        return []
    if isinstance(value, str):
        try:
            # Uploads can combine JSON objects with a separator; decode complete
            # objects so delimiters inside JSON strings are never split.
            decoder = json.JSONDecoder()
            entries = []
            remaining = value.strip()
            while remaining:
                parsed, end = decoder.raw_decode(remaining)
                entries.extend(parent_context_entries(parsed))
                remaining = remaining[end:].strip()
                if remaining:
                    if remaining.startswith("||||"):
                        remaining = remaining[4:].strip()
                    elif remaining.startswith(";"):
                        remaining = remaining[1:].strip()
                    else:
                        raise ValueError("Unexpected text after parentContext JSON")
            return entries
        except (ValueError, TypeError) as exc:
            raise ValueError(f"Invalid parentContext JSON: {exc}") from exc
    if isinstance(value, list):
        return [entry for item in value for entry in parent_context_entries(item)]
    if not isinstance(value, dict):
        raise ValueError("parentContext entry must be a JSON object")
    return [value]


def validate_parent_context_events(value, allowed_values, *, normalize_legacy=False, require_parent=True):
    entries = [dict(entry) for entry in parent_context_entries(value)]
    for entry in entries:
        canonical = validate_event_type(entry.get("eventType"), allowed_values,
                                        normalize_legacy=normalize_legacy)
        if canonical is not None and canonical != entry.get("eventType"):
            entry["eventType"] = canonical
        parent = entry.get("parent")
        if require_parent and not normalize_legacy and (not isinstance(parent, str) or not parent.strip()):
            raise ValueError("parentContext parent must be a nonempty CMID string")
        if not is_empty_event_value(entry.get("eventDate")) and is_empty_event_value(entry.get("eventType")):
            # Legacy date-only entries contribute no event. New writes reject
            # them, while reconciliation clears obsolete derived event data.
            if not normalize_legacy:
                raise ValueError("eventType is required when eventDate exists")
    return entries


def validate_event_rows(rows, driver, *, normalize_legacy=False):
    """Validate all input rows before a write, including embedded JSON values."""
    rows = list(rows)
    if not any("parentContext" in row or "eventType" in row for row in rows):
        return
    allowed = get_allowed_event_types(driver)
    for index, row in enumerate(rows):
        try:
            if "eventType" in row:
                value = row["eventType"]
                for item in value if isinstance(value, list) else [value]:
                    validate_event_type(item, allowed, normalize_legacy=normalize_legacy)
            if "parentContext" in row:
                validate_parent_context_events(row["parentContext"], allowed,
                                               normalize_legacy=normalize_legacy)
        except ValueError as exc:
            raise ValueError(f"Row {row.get('_spreadsheet_row', index + 1)}: {exc}") from exc


def validate_stored_event_contexts(driver, cmids=None):
    rows = getQuery("""
        MATCH (:DATASET)-[r:USES]->(c:CATEGORY)
        WHERE ($cmids IS NULL OR c.CMID IN $cmids)
          AND (r.parentContext IS NOT NULL OR r.eventType IS NOT NULL)
        RETURN r.parentContext AS parentContext, r.eventType AS eventType
    """, driver, params={"cmids": cmids})
    validate_event_rows(rows, driver, normalize_legacy=True)

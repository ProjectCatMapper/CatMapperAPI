import ast
import re


def _parse_row_list(raw_rows):
    if not raw_rows:
        return []
    try:
        parsed = ast.literal_eval(raw_rows)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    rows = []
    for value in parsed:
        try:
            rows.append(int(value))
        except Exception:
            continue
    return rows


def extract_upload_error_details(error_message):
    text = str(error_message or "").strip()
    if not text:
        return []

    details = []

    key_format_match = re.search(
        r"Invalid '([^']+)' format in rows:\s*\n?\s*(\[[^\]]*\])",
        text,
        flags=re.IGNORECASE,
    )
    if key_format_match:
        field = key_format_match.group(1)
        rows = _parse_row_list(key_format_match.group(2))
        if rows:
            for row in rows:
                details.append(
                    {
                        "row": row,
                        "field": field,
                        "code": "invalid_format",
                        "message": text,
                    }
                )
            return details

    for line in [ln.strip() for ln in text.splitlines()]:
        if line.endswith("must be in dataset"):
            field = line.replace(" must be in dataset", "").strip()
            details.append(
                {
                    "row": None,
                    "field": field,
                    "code": "missing_column",
                    "message": line,
                }
            )
    if details:
        return details

    property_exists_match = re.search(r"Property '([^']+)' already exists", text)
    if property_exists_match:
        details.append(
            {
                "row": None,
                "field": property_exists_match.group(1),
                "code": "already_exists",
                "message": text,
            }
        )

    return details

"""Name the source record behind a validation failure without inventing rows."""


def validation_location(record, number=None, *, spreadsheet=False):
    if spreadsheet or record.get("_spreadsheet_row") is not None:
        return f"Row {record.get('_spreadsheet_row', number)}"

    identifiers = []
    cmid = record.get("_source_cmid") or record.get("CMID")
    if cmid:
        identifiers.append(f"CMID={cmid}")
    for field in ("datasetID", "Key", "relID"):
        value = record.get(field)
        if value is not None and (value != "" or field == "Key"):
            identifiers.append(f"{field}={value!r}" if field == "Key" else f"{field}={value}")
    if identifiers:
        kind = "USES tie" if any(field in record for field in ("datasetID", "Key", "relID")) else "Category"
        return f"{kind} ({', '.join(identifiers)})"
    return f"Input record {number}" if number is not None else "Input record"

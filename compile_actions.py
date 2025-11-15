"""Compile action argument metadata from the raw Shortcuts SQLite database."""

from __future__ import annotations

import argparse
import base64
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Tuple

ACTION_NOTES: Mapping[str, List[str]] = {
    "is.workflow.actions.conditional": [
        (
            "This control-flow action must be paired with a matching `Conditional` "
            '"End If" action that shares the same `GroupingIdentifier`.'
        ),
        (
            "Insert another `Conditional` block with `WFControlFlowMode = 1` to add "
            "an `Otherwise` branch between the opening and closing blocks."
        ),
    ],
    "is.workflow.actions.repeat.count": [
        (
            "Repeat X Times requires an opening block and an `End Repeat` block. "
            "Both actions share a `GroupingIdentifier` and use `WFControlFlowMode` "
            "to signal whether they open (0) or close (1) the loop."
        ),
        (
            "Use the `Repeat Index` magic variable to access the current iteration number, "
            "and `Repeat Item` to work with the loop's input value."
        ),
    ],
    "is.workflow.actions.repeat.each": [
        (
            "Repeat with Each loops also require a closing `End Repeat` block that "
            "shares the same `GroupingIdentifier` but sets `WFControlFlowMode = 1`."
        ),
        (
            "Inside the loop, `Repeat Item` exposes the element currently being processed; "
            "`Repeat Index` tracks the 1-based loop counter."
        ),
    ],
    "is.workflow.actions.dictionary": [
        (
            "Provide dictionary data via the `WFItems` parameter. When building shortcuts with "
            "`js-to-shortcut`, call `serialization.dictionary({ key: value })` and assign the "
            "result to `WFItems` (for example: `action('is.workflow.actions.dictionary', "
            "{ WFItems: serialization.dictionary({ name: 'Value' }) })`)."
        ),
        (
            "Each entry is stored as a `WFDictionaryFieldValueItem` with an inferred `WFItemType` "
            "— strings map to type 0, objects to 1, arrays to 2, numbers to 3, and booleans to 4. "
            "The serialization helper handles these conversions automatically."
        ),
    ],
}


def encode_blob(value: bytes | memoryview | None) -> str | None:
    """Return a base64 string for blobs so they survive JSON serialization."""
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = value.tobytes()
    if not value:
        return ""
    return base64.b64encode(value).decode("ascii")


def _read_varint(buffer: memoryview, pos: int, end: int) -> Tuple[int, int]:
    """Decode a protobuf-style varint starting at pos."""
    shift = 0
    value = 0
    while pos < end:
        byte = buffer[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte & 0x80:
            shift += 7
            continue
        return value, pos
    raise ValueError("Unexpected end of buffer while decoding varint")


def decode_requirements(blob: bytes | memoryview | None) -> List[int]:
    """Parse requirement blobs into a list of integer requirement identifiers."""

    if not blob:
        return []

    view = memoryview(blob)
    end = len(view)

    def parse_segment(start: int, stop: int) -> List[int]:
        values: List[int] = []
        pos = start
        while pos < stop:
            tag = view[pos]
            pos += 1
            wire_type = tag & 0x07
            if wire_type == 0:  # varint
                val, pos = _read_varint(view, pos, stop)
                values.append(val)
            elif wire_type == 1:  # 64-bit
                pos += 8
            elif wire_type == 2:  # length-delimited
                length, pos = _read_varint(view, pos, stop)
                sub_end = min(pos + length, stop)
                values.extend(parse_segment(pos, sub_end))
                pos = sub_end
            elif wire_type == 5:  # 32-bit
                pos += 4
            elif wire_type in (3, 4):  # deprecated start/end group
                # These shouldn't appear, but break to avoid infinite loops.
                break
            else:
                break
        return values

    return parse_segment(0, end)


def load_tool_output_types(db_path: Path) -> Dict[int, List[str]]:
    """Load tool output type identifiers from ToolOutputTypes.json if present."""

    output_path = db_path.with_name("ToolOutputTypes.json")
    if not output_path.exists():
        return {}

    try:
        payload = json.loads(output_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse tool output types: {exc}") from exc

    grouped: MutableMapping[int, List[str]] = defaultdict(list)
    for entry in payload:
        tool_id = entry.get("toolId")
        type_identifier = entry.get("typeIdentifier")
        if tool_id is None or not type_identifier:
            continue
        grouped[int(tool_id)].append(type_identifier)

    return dict(grouped)


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})")
        if row["name"]
    }


def fetch_tool_rows(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            rowId,
            id,
            toolType,
            flags,
            visibilityFlags,
            requirements,
            authenticationPolicy,
            customIcon,
            deprecationReplacementId,
            sourceActionProvider,
            outputTypeInstance,
            sourceContainerId,
            attributionContainerId
        FROM Tools
        WHERE toolType = 'action'
        ORDER BY rowId
        """
    ).fetchall()


def fetch_tool_localizations(
    conn: sqlite3.Connection, locale: str, fallback_locale: str | None = "en"
) -> Dict[int, sqlite3.Row]:
    def query(loc: str) -> List[sqlite3.Row]:
        return conn.execute(
            """
            SELECT toolId, locale, name, descriptionSummary
            FROM ToolLocalizations
            WHERE locale = ?
              AND toolId IN (SELECT rowId FROM Tools WHERE toolType = 'action')
            """,
            (loc,),
        ).fetchall()

    localized: Dict[int, sqlite3.Row] = {}
    for row in query(locale):
        localized[row["toolId"]] = row

    if fallback_locale and fallback_locale != locale:
        for row in query(fallback_locale):
            localized.setdefault(row["toolId"], row)

    return localized


def fetch_parameter_localizations(
    conn: sqlite3.Connection, locale: str, fallback_locale: str | None = "en"
) -> Dict[Tuple[int, str], sqlite3.Row]:
    def query(loc: str) -> List[sqlite3.Row]:
        return conn.execute(
            """
            SELECT toolId, key, locale, name, description
            FROM ParameterLocalizations
            WHERE locale = ?
              AND toolId IN (SELECT rowId FROM Tools WHERE toolType = 'action')
            """,
            (loc,),
        ).fetchall()

    localized: Dict[Tuple[int, str], sqlite3.Row] = {}
    for row in query(locale):
        localized[(row["toolId"], row["key"])] = row

    if fallback_locale and fallback_locale != locale:
        for row in query(fallback_locale):
            localized.setdefault((row["toolId"], row["key"]), row)

    return localized


def fetch_parameters(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    parameter_columns = table_columns(conn, "Parameters")
    type_columns = table_columns(conn, "Types")

    def add_param_column(column: str, default: str = "NULL") -> str:
        if column in parameter_columns:
            return f"Parameters.{column} AS {column}"
        return f"{default} AS {column}"

    select_parts = [
        "Parameters.rowid AS parameterRowId",
        "Parameters.toolId",
        add_param_column("key"),
        add_param_column("sortOrder", "0"),
        add_param_column("relationships"),
        add_param_column("flags", "0"),
        add_param_column("typeInstance"),
    ]

    has_type_join = "typeId" in parameter_columns and type_columns
    if has_type_join:
        select_parts.append("Parameters.typeId AS typeId")
    else:
        select_parts.append("NULL AS typeId")

    def add_type_column(column: str, alias: str) -> str:
        if has_type_join and column in type_columns:
            return f"Types.{column} AS {alias}"
        return f"NULL AS {alias}"

    select_parts.extend(
        [
            add_type_column("id", "typeIdentifier"),
            add_type_column("kind", "typeKind"),
            add_type_column("runtimeFlags", "typeRuntimeFlags"),
            add_type_column("runtimeRequirements", "typeRuntimeRequirements"),
        ]
    )

    select_sql = ",\n            ".join(select_parts)
    type_join = (
        "LEFT JOIN Types ON Types.rowId = Parameters.typeId" if has_type_join else ""
    )

    sort_order_column = (
        "Parameters.sortOrder" if "sortOrder" in parameter_columns else "Parameters.rowid"
    )
    key_order_column = (
        "Parameters.key" if "key" in parameter_columns else "Parameters.rowid"
    )

    query = f"""
        SELECT
            {select_sql}
        FROM Parameters
        JOIN Tools ON Tools.rowId = Parameters.toolId
        {type_join}
        WHERE Tools.toolType = 'action'
        ORDER BY Parameters.toolId, {sort_order_column}, {key_order_column}
        """
    return conn.execute(query).fetchall()


def build_payload(
    tools: Iterable[sqlite3.Row],
    tool_localizations: Mapping[int, sqlite3.Row],
    parameter_localizations: Mapping[Tuple[int, str], sqlite3.Row],
    parameters: Iterable[sqlite3.Row],
    tool_output_types: Mapping[int, List[str]],
) -> Mapping[str, dict]:
    params_by_tool: MutableMapping[int, List[dict]] = defaultdict(list)
    for param in parameters:
        localized = parameter_localizations.get((param["toolId"], param["key"]))
        params_by_tool[param["toolId"]].append(
            {
                "rowId": param["parameterRowId"],
                "key": param["key"],
                "name": (localized["name"] if localized else None) or param["key"],
                "description": localized["description"] if localized else None,
                "sortOrder": param["sortOrder"],
                "flags": param["flags"],
                "relationships": encode_blob(param["relationships"]),
                "typeInstance": encode_blob(param["typeInstance"]),
                "type": {
                    "rowId": param["typeId"],
                    "kind": param["typeKind"],
                    "runtimeFlags": param["typeRuntimeFlags"],
                    "runtimeRequirements": encode_blob(param["typeRuntimeRequirements"]),
                    "encodedId": encode_blob(param["typeIdentifier"]),
                },
            }
        )

    compiled: Dict[str, dict] = {}
    for tool in tools:
        localized = tool_localizations.get(tool["rowId"])
        requirements_blob = tool["requirements"]
        output_types = tool_output_types.get(tool["rowId"])
        compiled[tool["id"]] = {
            "rowId": tool["rowId"],
            "name": (localized["name"] if localized else None) or tool["id"],
            "description": localized["descriptionSummary"] if localized else None,
            "toolType": tool["toolType"],
            "sourceActionProvider": tool["sourceActionProvider"],
            "sourceContainerId": tool["sourceContainerId"],
            "attributionContainerId": tool["attributionContainerId"],
            "flags": tool["flags"],
            "visibilityFlags": tool["visibilityFlags"],
            "authenticationPolicy": tool["authenticationPolicy"],
            "deprecationReplacementId": tool["deprecationReplacementId"],
            "requirements": {
                "raw": encode_blob(requirements_blob),
                "decoded": decode_requirements(requirements_blob),
            },
            "outputTypeInstance": encode_blob(tool["outputTypeInstance"]),
            "customIcon": encode_blob(tool["customIcon"]),
            "arguments": params_by_tool.get(tool["rowId"], []),
            "notes": ACTION_NOTES.get(tool["id"]),
        }
        if output_types:
            compiled[tool["id"]]["outputTypes"] = output_types

    return compiled


def load_manual_overrides(db_path: Path) -> Dict[str, dict]:
    override_path = db_path.with_name("manual-overrides.json")
    if not override_path.exists():
        return {}

    try:
        overrides = json.loads(override_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Failed to parse manual overrides: {exc}") from exc

    normalized: Dict[str, dict] = {}
    for action_id, override in overrides.items():
        if not isinstance(override, dict):
            continue
        normalized[action_id] = override
    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compile per-action argument metadata from the Shortcuts raw.sqlite export."
        )
    )
    parser.add_argument(
        "--db",
        default="actions/MacOS-15.4/raw.sqlite",
        help="Path to the raw.sqlite database (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default="compiled.json",
        help="Destination for the compiled JSON (default: %(default)s)",
    )
    parser.add_argument(
        "--locale",
        default="en",
        help="Preferred locale for localized strings (default: %(default)s)",
    )
    parser.add_argument(
        "--fallback-locale",
        default="en",
        help="Fallback locale when a string is missing (default: %(default)s)",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        tools = fetch_tool_rows(conn)
        if not tools:
            raise SystemExit("No action tools found in the database.")

        tool_localizations = fetch_tool_localizations(
            conn, args.locale, fallback_locale=args.fallback_locale
        )
        parameter_localizations = fetch_parameter_localizations(
            conn, args.locale, fallback_locale=args.fallback_locale
        )
        parameters = fetch_parameters(conn)
    finally:
        conn.close()

    tool_output_types = load_tool_output_types(db_path)

    compiled_actions = build_payload(
        tools, tool_localizations, parameter_localizations, parameters, tool_output_types
    )

    overrides = load_manual_overrides(db_path)
    for action_id, override in overrides.items():
        if action_id in compiled_actions:
            continue
        compiled_actions[action_id] = override

    payload = {
        "metadata": {
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "locale": args.locale,
            "fallbackLocale": args.fallback_locale,
            "sourceDatabase": str(db_path),
            "actionCount": len(compiled_actions),
        },
        "actions": compiled_actions,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

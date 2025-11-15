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
    return conn.execute(
        """
        SELECT
            Parameters.rowid AS parameterRowId,
            Parameters.toolId,
            Parameters.key,
            Parameters.sortOrder,
            Parameters.relationships,
            Parameters.flags,
            Parameters.typeId,
            Parameters.typeInstance,
            Types.id AS typeIdentifier,
            Types.kind AS typeKind,
            Types.runtimeFlags AS typeRuntimeFlags,
            Types.runtimeRequirements AS typeRuntimeRequirements
        FROM Parameters
        JOIN Tools ON Tools.rowId = Parameters.toolId
        LEFT JOIN Types ON Types.rowId = Parameters.typeId
        WHERE Tools.toolType = 'action'
        ORDER BY Parameters.toolId, Parameters.sortOrder, Parameters.key
        """
    ).fetchall()


def build_payload(
    tools: Iterable[sqlite3.Row],
    tool_localizations: Mapping[int, sqlite3.Row],
    parameter_localizations: Mapping[Tuple[int, str], sqlite3.Row],
    parameters: Iterable[sqlite3.Row],
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
        }

    return compiled


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

    compiled_actions = build_payload(
        tools, tool_localizations, parameter_localizations, parameters
    )

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

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
            "requirements": encode_blob(tool["requirements"]),
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

#!/usr/bin/env python3
"""Best-effort decoder for WorkflowKit action metadata."""

from __future__ import annotations

import argparse
import json
import plistlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Mapping


WORKFLOWKIT_MARKER = re.compile(r"^WorkflowKit\d+$")
CLASS_PATTERN = re.compile(r"^WF[A-Za-z0-9]+(?:Action|Intent)$")
KEYWORDS_PATTERN = re.compile(r"[|]")
ICON_PATTERN = re.compile(r"^[a-z0-9.]+$")
PARAMETER_TYPE_PATTERN = re.compile(r"^WF[A-Za-z0-9]+Parameter$")
PARAMETER_KEY_PATTERN = re.compile(r"^WF[A-Za-z0-9]+$")
LOCALIZATION_KEY_PATTERN = re.compile(r"^(?P<label>.+?) \((?P<scope>[^)]+)\)$")


def load_localizable_strings(path: Path) -> Dict[str, str]:
    """Parse the binary Localizable.strings file if it exists."""
    if not path.exists():
        return {}
    try:
        return plistlib.loads(path.read_bytes())
    except Exception as exc:  # pragma: no cover - defensive
        raise SystemExit(f"Failed to parse {path}: {exc}") from exc


def iter_null_terminated_strings(data: bytes) -> Iterator[str]:
    """Yield UTF-8-ish strings split on NUL bytes."""
    for chunk in data.split(b"\x00"):
        if not chunk:
            continue
        try:
            text = chunk.decode("utf-8")
        except UnicodeDecodeError:
            text = chunk.decode("latin-1", errors="ignore")
        text = text.strip()
        if text:
            yield text


@dataclass
class ActionBlock:
    identifier: str
    tokens: List[str]


def extract_action_blocks(data: bytes) -> List[ActionBlock]:
    """Split the WorkflowKit binary into logical blocks keyed by WorkflowKitN markers."""
    tokens = list(iter_null_terminated_strings(data))
    blocks: List[ActionBlock] = []
    current_id: str | None = None
    current_tokens: List[str] = []

    for token in tokens:
        if WORKFLOWKIT_MARKER.match(token):
            if current_id is not None:
                blocks.append(ActionBlock(current_id, current_tokens))
            current_id = token
            current_tokens = []
        elif current_id is not None:
            current_tokens.append(token)

    if current_id is not None:
        blocks.append(ActionBlock(current_id, current_tokens))

    return blocks


def canonical_scope(scope: str) -> str:
    """Normalize scope strings to help match parameter keys."""
    if scope.startswith("WF"):
        return scope.split("(", 1)[0]
    return scope


def parse_action_block(
    block: ActionBlock, translations: Mapping[str, str]
) -> Dict[str, object]:
    """Best-effort interpretation of a block's tokens."""
    action: Dict[str, object] = {"id": block.identifier}
    tokens = block.tokens

    class_name = next((t for t in tokens if CLASS_PATTERN.match(t)), None)
    if class_name:
        action["className"] = class_name

    keyword_str = next(
        (t for t in tokens if KEYWORDS_PATTERN.search(t) and " " not in t), None
    )
    if keyword_str:
        action["keywords"] = [part for part in keyword_str.split("|") if part]

    icon_name = next(
        (
            t
            for t in tokens
            if ICON_PATTERN.match(t)
            and "." in t
            and " " not in t
            and not t.startswith("WF")
        ),
        None,
    )
    if icon_name:
        action["icon"] = icon_name

    first_localized_index = next(
        (idx for idx, token in enumerate(tokens) if LOCALIZATION_KEY_PATTERN.match(token)),
        len(tokens),
    )
    descriptive = []
    for token in tokens[:first_localized_index]:
        if token in (class_name, keyword_str, icon_name):
            continue
        if PARAMETER_TYPE_PATTERN.match(token):
            break
        descriptive.append(token)
    if descriptive:
        action["descriptions"] = descriptive

    localizations: Dict[str, List[Dict[str, str]]] = {}
    parameters: List[Dict[str, object]] = []
    scope_to_param: Dict[str, Dict[str, object]] = {}

    pending_param: Dict[str, object] | None = None
    i = 0
    while i < len(tokens):
        token = tokens[i]

        if PARAMETER_TYPE_PATTERN.match(token):
            pending_param = {"type": token}
            parameters.append(pending_param)
            i += 1
            continue

        if (
            pending_param
            and pending_param.get("key") is None
            and PARAMETER_KEY_PATTERN.match(token)
        ):
            pending_param["key"] = token
            scope_to_param[token] = pending_param
            i += 1
            continue

        match = LOCALIZATION_KEY_PATTERN.match(token)
        if match:
            label = match.group("label").strip()
            scope = match.group("scope").strip()
            value = translations.get(token)
            consumed_extra = False
            if value is None and i + 1 < len(tokens):
                candidate = tokens[i + 1]
                if not LOCALIZATION_KEY_PATTERN.match(candidate) and not PARAMETER_TYPE_PATTERN.match(candidate):
                    value = candidate
                    consumed_extra = True
            canon = canonical_scope(scope)
            target_param = scope_to_param.get(canon)

            if not target_param and scope.startswith("WF"):
                for param in reversed(parameters):
                    if param.get("key") in (None, canon):
                        param["key"] = canon
                        scope_to_param[canon] = param
                        target_param = param
                        break

            entry = {"key": label, "value": value, "scope": scope}
            if target_param:
                target_param.setdefault("localizations", []).append(entry)
            else:
                localizations.setdefault(scope, []).append(entry)

            i += 2 if consumed_extra else 1
            continue

        if token == "RequiredResources":
            resources: List[str] = []
            j = i + 1
            while j < len(tokens):
                candidate = tokens[j]
                if LOCALIZATION_KEY_PATTERN.match(candidate) or candidate == "RequiredResources":
                    break
                if candidate.startswith("WF"):
                    resources.append(candidate)
                j += 1
            if resources:
                action["requiredResources"] = resources
            i = j
            continue

        i += 1

    if localizations:
        action["localizations"] = localizations
    if parameters:
        action["parameters"] = parameters

    return action


def decode_workflowkit(
    workflowkit_path: Path, translations: Mapping[str, str]
) -> List[Dict[str, object]]:
    data = workflowkit_path.read_bytes()
    blocks = extract_action_blocks(data)
    parsed = [parse_action_block(block, translations) for block in blocks]
    parsed.sort(key=lambda item: int(item["id"].replace("WorkflowKit", "")))
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Decode WorkflowKit resources into JSON."
    )
    parser.add_argument(
        "--workflowkit",
        default="workflowkit/WorkflowKit",
        help="Path to the WorkflowKit binary (default: %(default)s)",
    )
    parser.add_argument(
        "--strings",
        default="workflowkit/Localizable.strings",
        help="Path to Localizable.strings (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default="workflowkit/actions.decoded.json",
        help="Destination JSON for decoded actions (default: %(default)s)",
    )
    parser.add_argument(
        "--strings-output",
        default="workflowkit/localizations.json",
        help="Optional JSON dump of Localizable.strings (default: %(default)s)",
    )
    args = parser.parse_args()

    workflowkit_path = Path(args.workflowkit)
    if not workflowkit_path.exists():
        raise SystemExit(f"WorkflowKit binary not found: {workflowkit_path}")

    localizable_map = load_localizable_strings(Path(args.strings))
    actions = decode_workflowkit(workflowkit_path, localizable_map)
    if localizable_map:
        strings_output = Path(args.strings_output)
        strings_output.parent.mkdir(parents=True, exist_ok=True)
        strings_output.write_text(
            json.dumps(localizable_map, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    payload = {
        "metadata": {
            "source": str(workflowkit_path),
            "actionCount": len(actions),
            "stringsCount": len(localizable_map),
        },
        "actions": actions,
    }
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
    )


if __name__ == "__main__":
    main()

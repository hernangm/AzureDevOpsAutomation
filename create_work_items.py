#!/usr/bin/env python3
"""
Azure DevOps Work Item Automation Script

Creates Epics, Features, and Tasks in Azure DevOps from a structured JSON
project plan file. Supports dry-run mode, duplicate detection, automatic
parent linking, and feature-owner task assignment.

Usage:
    python create_work_items.py -i project_plan.json --dry-run
    python create_work_items.py -i project_plan.json -v
"""

import argparse
import json
import logging
import sys
import time

import requests
from dotenv import dotenv_values
from jsonschema import ValidationError, validate

logger = logging.getLogger("ado-workitems")

SCHEMA_PATH = "project_plan_schema.json"


# --------------------------------------------------------------------------- #
#  Configuration
# --------------------------------------------------------------------------- #


def load_config(env_file: str) -> dict:
    """Load and validate configuration from a .env file."""
    values = dotenv_values(env_file)

    required = {
        "AZURE_DEVOPS_ORG_URL": "Organization URL (e.g. https://dev.azure.com/myorg)",
        "AZURE_DEVOPS_PROJECT": "Project name",
        "AZURE_DEVOPS_PAT": "Personal Access Token",
    }

    missing = [
        f"  {key} — {desc}"
        for key, desc in required.items()
        if not values.get(key, "").strip()
    ]

    if missing:
        logger.error(
            "Missing required configuration in %s:\n%s",
            env_file,
            "\n".join(missing),
        )
        sys.exit(2)

    return {
        "org_url": values["AZURE_DEVOPS_ORG_URL"].rstrip("/"),
        "project": values["AZURE_DEVOPS_PROJECT"].strip(),
        "pat": values["AZURE_DEVOPS_PAT"].strip(),
    }


# --------------------------------------------------------------------------- #
#  Input loading and validation
# --------------------------------------------------------------------------- #


def _load_schema(schema_path: str) -> dict:
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error("Schema file not found: %s", schema_path)
        sys.exit(2)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in schema file %s: %s", schema_path, exc)
        sys.exit(2)


def load_and_validate_input(filepath: str, schema_path: str) -> dict:
    """Load JSON input file and validate against the schema."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.error("Input file not found: %s", filepath)
        sys.exit(2)
    except json.JSONDecodeError as exc:
        logger.error("Invalid JSON in %s: %s", filepath, exc)
        sys.exit(2)

    schema = _load_schema(schema_path)

    try:
        validate(instance=data, schema=schema)
    except ValidationError as exc:
        logger.error("Schema validation failed:\n  %s", exc.message)
        sys.exit(2)

    # Check for duplicate IDs across the entire plan
    seen_ids: dict[str, str] = {}
    errors: list[str] = []
    for epic in data["epics"]:
        if epic["id"] in seen_ids:
            errors.append(f"Duplicate ID: {epic['id']}")
        seen_ids[epic["id"]] = "Epic"
        for feature in epic["features"]:
            if feature["id"] in seen_ids:
                errors.append(f"Duplicate ID: {feature['id']}")
            seen_ids[feature["id"]] = "Feature"
            for task in feature["tasks"]:
                if task["id"] in seen_ids:
                    errors.append(f"Duplicate ID: {task['id']}")
                seen_ids[task["id"]] = "Task"

    # Enforce assignment strategy contract
    strategy = data["metadata"].get("assignmentStrategy", "feature-owner")
    if strategy == "feature-owner":
        for epic in data["epics"]:
            for feature in epic["features"]:
                for task in feature["tasks"]:
                    if "assignedTo" in task:
                        errors.append(
                            f'[{task["id"]}] task.assignedTo is not allowed '
                            f'when assignmentStrategy is "feature-owner"'
                        )

    if errors:
        logger.error("Input validation errors:\n  %s", "\n  ".join(errors))
        sys.exit(2)

    return data


# --------------------------------------------------------------------------- #
#  Azure DevOps REST API Client
# --------------------------------------------------------------------------- #


class AzureDevOpsError(Exception):
    """Raised when an Azure DevOps API call fails."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"HTTP {status_code}: {message}")


class AzureDevOpsClient:
    """Thin wrapper around requests for Azure DevOps REST API calls."""

    API_VERSION = "7.1"
    MAX_RETRIES = 3

    def __init__(self, org_url: str, project: str, pat: str):
        self.org_url = org_url
        self.project = project
        self.session = requests.Session()
        self.session.auth = ("", pat)

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an HTTP request with retry logic for rate limiting and network errors."""
        for attempt in range(self.MAX_RETRIES + 1):
            try:
                resp = self.session.request(method, url, **kwargs)

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2**attempt))
                    logger.warning(
                        "Rate limited. Retrying after %ds (attempt %d/%d)...",
                        retry_after,
                        attempt + 1,
                        self.MAX_RETRIES,
                    )
                    time.sleep(retry_after)
                    continue

                return resp

            except requests.ConnectionError:
                if attempt == self.MAX_RETRIES:
                    raise
                wait = 2**attempt
                logger.warning(
                    "Connection error. Retrying in %ds (attempt %d/%d)...",
                    wait,
                    attempt + 1,
                    self.MAX_RETRIES,
                )
                time.sleep(wait)

        return resp  # type: ignore[possibly-undefined]

    def find_existing_work_item(
        self, title: str, work_item_type: str
    ) -> int | None:
        """Check if a work item with the exact title and type already exists.

        Returns the work item ID if found, None otherwise.
        """
        escaped_title = title.replace("'", "''")
        wiql = (
            f"SELECT [System.Id] FROM WorkItems "
            f"WHERE [System.TeamProject] = '{self.project}' "
            f"AND [System.WorkItemType] = '{work_item_type}' "
            f"AND [System.Title] = '{escaped_title}'"
        )
        url = (
            f"{self.org_url}/{self.project}/_apis/wit/wiql"
            f"?api-version={self.API_VERSION}"
        )

        resp = self._request(
            "POST",
            url,
            json={"query": wiql},
            headers={"Content-Type": "application/json"},
        )

        if resp.status_code in (401, 403):
            raise AzureDevOpsError(
                resp.status_code,
                "Authentication failed. Check your PAT and its permissions.",
            )

        resp.raise_for_status()
        work_items = resp.json().get("workItems", [])
        return work_items[0]["id"] if work_items else None

    def create_work_item(
        self,
        work_item_type: str,
        title: str,
        description: str | None = None,
        assigned_to: str | None = None,
        parent_id: int | None = None,
        custom_fields: dict | None = None,
    ) -> dict:
        """Create a single work item and return the API response dict."""
        patch_doc = [
            {"op": "add", "path": "/fields/System.Title", "value": title}
        ]

        if description:
            patch_doc.append(
                {
                    "op": "add",
                    "path": "/fields/System.Description",
                    "value": description,
                }
            )

        if assigned_to:
            patch_doc.append(
                {
                    "op": "add",
                    "path": "/fields/System.AssignedTo",
                    "value": assigned_to,
                }
            )

        if custom_fields:
            for field_name, field_value in custom_fields.items():
                patch_doc.append(
                    {
                        "op": "add",
                        "path": f"/fields/{field_name}",
                        "value": field_value,
                    }
                )

        if parent_id is not None:
            patch_doc.append(
                {
                    "op": "add",
                    "path": "/relations/-",
                    "value": {
                        "rel": "System.LinkTypes.Hierarchy-Reverse",
                        "url": f"{self.org_url}/_apis/wit/workItems/{parent_id}",
                        "attributes": {
                            "comment": "Auto-linked by create_work_items script"
                        },
                    },
                }
            )

        url = (
            f"{self.org_url}/{self.project}/_apis/wit/workitems"
            f"/${work_item_type}?api-version={self.API_VERSION}"
        )

        logger.debug("POST %s\n%s", url, json.dumps(patch_doc, indent=2))

        resp = self._request(
            "POST",
            url,
            json=patch_doc,
            headers={"Content-Type": "application/json-patch+json"},
        )

        if resp.status_code in (401, 403):
            raise AzureDevOpsError(
                resp.status_code,
                "Authentication failed. Check your PAT and its permissions.",
            )

        if resp.status_code >= 400:
            error_msg = resp.text
            try:
                error_msg = resp.json().get("message", resp.text)
            except (ValueError, KeyError):
                pass
            raise AzureDevOpsError(resp.status_code, error_msg)

        return resp.json()


# --------------------------------------------------------------------------- #
#  Processing engine
# --------------------------------------------------------------------------- #


class Summary:
    """Tracks creation results."""

    def __init__(self):
        self.created = 0
        self.skipped = 0
        self.failed = 0
        self.failures: list[str] = []

    def record_created(self, local_id: str, title: str, ado_id: int, url: str):
        self.created += 1
        logger.info(
            "  CREATED  [%s] \"%s\" -> work item #%d (%s)", local_id, title, ado_id, url
        )

    def record_skipped(self, local_id: str, title: str, ado_id: int):
        self.skipped += 1
        logger.info(
            '  SKIPPED  [%s] "%s" — duplicate found (work item #%d)',
            local_id,
            title,
            ado_id,
        )

    def record_failed(self, local_id: str, title: str, error: str):
        self.failed += 1
        msg = f'[{local_id}] "{title}": {error}'
        self.failures.append(msg)
        logger.error("  FAILED   %s", msg)

    def record_dry_run(
        self,
        local_id: str,
        title: str,
        work_item_type: str,
        assigned_to: str | None,
        parent_label: str | None,
    ):
        self.created += 1
        parts = [f"[{local_id}]", f'"{title}"', f"({work_item_type})"]
        if assigned_to:
            parts.append(f"-> {assigned_to}")
        if parent_label:
            parts.append(f"under {parent_label}")
        logger.info("  DRY-RUN  %s", " ".join(parts))

    def print_report(self):
        print("\n=== Work Item Creation Summary ===")
        print(f"  Created:             {self.created}")
        print(f"  Skipped (duplicate): {self.skipped}")
        print(f"  Failed:              {self.failed}")
        if self.failures:
            print("\nFailures:")
            for f in self.failures:
                print(f"  - {f}")


def _get_work_item_url(org_url: str, project: str, ado_id: int) -> str:
    return f"{org_url}/{project}/_workitems/edit/{ado_id}"


def process_epics(
    client: AzureDevOpsClient,
    config: dict,
    epics: list[dict],
    dry_run: bool,
    skip_duplicate_check: bool,
) -> Summary:
    """Process the full Epic -> Feature -> Task tree."""
    summary = Summary()
    dry_counter = 0

    for epic in epics:
        epic_id_local = epic["id"]
        epic_title = epic["title"]
        epic_desc = epic.get("description")
        epic_fields = epic.get("fields")

        logger.info("Epic: [%s] %s", epic_id_local, epic_title)

        epic_ado_id = None

        # Duplicate check
        if not skip_duplicate_check and not dry_run:
            try:
                epic_ado_id = client.find_existing_work_item(epic_title, "Epic")
            except AzureDevOpsError as exc:
                if exc.status_code in (401, 403):
                    logger.error("Authentication failed: %s", exc.message)
                    sys.exit(1)

        if epic_ado_id is not None:
            summary.record_skipped(epic_id_local, epic_title, epic_ado_id)
        elif dry_run:
            dry_counter += 1
            summary.record_dry_run(
                epic_id_local, epic_title, "Epic", None, None
            )
        else:
            try:
                result = client.create_work_item(
                    "Epic", epic_title, description=epic_desc,
                    custom_fields=epic_fields,
                )
                epic_ado_id = result["id"]
                url = _get_work_item_url(
                    config["org_url"], config["project"], epic_ado_id
                )
                summary.record_created(epic_id_local, epic_title, epic_ado_id, url)
            except (AzureDevOpsError, requests.RequestException) as exc:
                summary.record_failed(epic_id_local, epic_title, str(exc))
                continue  # Skip features under this epic

        # Process features
        for feature in epic["features"]:
            feat_id_local = feature["id"]
            feat_title = feature["title"]
            feat_desc = feature.get("description")
            feat_fields = feature.get("fields")
            owner = feature["ownerUserIds"][0]  # feature-owner: first email

            logger.info("  Feature: [%s] %s", feat_id_local, feat_title)

            feat_ado_id = None

            if not skip_duplicate_check and not dry_run:
                try:
                    feat_ado_id = client.find_existing_work_item(
                        feat_title, "Feature"
                    )
                except (AzureDevOpsError, requests.RequestException) as exc:
                    summary.record_failed(feat_id_local, feat_title, str(exc))
                    continue

            if feat_ado_id is not None:
                summary.record_skipped(feat_id_local, feat_title, feat_ado_id)
            elif dry_run:
                dry_counter += 1
                summary.record_dry_run(
                    feat_id_local,
                    feat_title,
                    "Feature",
                    owner,
                    f"Epic [{epic_id_local}]",
                )
            else:
                try:
                    result = client.create_work_item(
                        "Feature",
                        feat_title,
                        description=feat_desc,
                        assigned_to=owner,
                        parent_id=epic_ado_id,
                        custom_fields=feat_fields,
                    )
                    feat_ado_id = result["id"]
                    url = _get_work_item_url(
                        config["org_url"], config["project"], feat_ado_id
                    )
                    summary.record_created(
                        feat_id_local, feat_title, feat_ado_id, url
                    )
                except (AzureDevOpsError, requests.RequestException) as exc:
                    summary.record_failed(feat_id_local, feat_title, str(exc))
                    continue  # Skip tasks under this feature

            # Process tasks
            for task in feature["tasks"]:
                task_id_local = task["id"]
                task_title = task["title"]
                task_desc = task.get("description")
                task_fields = task.get("fields")
                task_owner = owner  # feature-owner strategy: always inherit

                logger.info("    Task: [%s] %s", task_id_local, task_title)

                task_ado_id = None

                if not skip_duplicate_check and not dry_run:
                    try:
                        task_ado_id = client.find_existing_work_item(
                            task_title, "Task"
                        )
                    except (AzureDevOpsError, requests.RequestException) as exc:
                        summary.record_failed(task_id_local, task_title, str(exc))
                        continue

                if task_ado_id is not None:
                    summary.record_skipped(
                        task_id_local, task_title, task_ado_id
                    )
                elif dry_run:
                    dry_counter += 1
                    summary.record_dry_run(
                        task_id_local,
                        task_title,
                        "Task",
                        task_owner,
                        f"Feature [{feat_id_local}]",
                    )
                else:
                    try:
                        result = client.create_work_item(
                            "Task",
                            task_title,
                            description=task_desc,
                            assigned_to=task_owner,
                            parent_id=feat_ado_id,
                            custom_fields=task_fields,
                        )
                        task_ado_id = result["id"]
                        url = _get_work_item_url(
                            config["org_url"], config["project"], task_ado_id
                        )
                        summary.record_created(
                            task_id_local, task_title, task_ado_id, url
                        )
                    except (AzureDevOpsError, requests.RequestException) as exc:
                        summary.record_failed(task_id_local, task_title, str(exc))

    return summary


# --------------------------------------------------------------------------- #
#  CLI
# --------------------------------------------------------------------------- #


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create Azure DevOps work items from a JSON project plan.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  %(prog)s -i plan.json --dry-run    Preview without creating\n"
            "  %(prog)s -i plan.json -v            Create with verbose output\n"
        ),
    )
    parser.add_argument(
        "-i",
        "--input",
        required=True,
        help="Path to the JSON input file",
    )
    parser.add_argument(
        "-s",
        "--schema",
        default=SCHEMA_PATH,
        help=f"Path to the JSON schema file (default: {SCHEMA_PATH})",
    )
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        default=False,
        help="Preview what would be created without calling the API",
    )
    parser.add_argument(
        "-e",
        "--env-file",
        default=".env",
        help="Path to the .env file (default: .env)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Enable debug-level logging",
    )
    parser.add_argument(
        "--no-duplicate-check",
        action="store_true",
        default=False,
        help="Skip duplicate detection for faster execution",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Load and validate
    logger.info("Loading input from %s ...", args.input)
    data = load_and_validate_input(args.input, args.schema)
    logger.info(
        "Validated: %d epic(s), %d feature(s), %d task(s)",
        len(data["epics"]),
        sum(len(e["features"]) for e in data["epics"]),
        sum(
            len(t["tasks"])
            for e in data["epics"]
            for t in e["features"]
        ),
    )

    if args.dry_run:
        logger.info("\n*** DRY RUN — no work items will be created ***\n")
        client = None  # type: ignore[assignment]
        config = {"org_url": "", "project": "", "pat": ""}
    else:
        config = load_config(args.env_file)
        client = AzureDevOpsClient(
            config["org_url"], config["project"], config["pat"]
        )

    # Process
    summary = process_epics(
        client,
        config,
        data["epics"],
        dry_run=args.dry_run,
        skip_duplicate_check=args.no_duplicate_check,
    )

    summary.print_report()

    if summary.failed > 0:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()

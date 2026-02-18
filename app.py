"""Streamlit app for Azure DevOps Work Item Automation."""

import json

import streamlit as st

from create_work_items import (
    SCHEMA_PATH,
    AzureDevOpsClient,
    AzureDevOpsError,
    Summary,
    build_config,
    process_epics,
    validate_input,
)

st.set_page_config(page_title="Azure DevOps Work Items", page_icon=":clipboard:")
st.title("Azure DevOps Work Item Creator")


# ── Config from st.secrets ──────────────────────────────────────────────────

def _get_config() -> dict:
    """Read Azure DevOps config from Streamlit secrets."""
    return build_config(
        st.secrets["AZURE_DEVOPS_ORG_URL"],
        st.secrets["AZURE_DEVOPS_PROJECT"],
        st.secrets["AZURE_DEVOPS_PAT"],
    )


# ── File upload ─────────────────────────────────────────────────────────────

uploaded = st.file_uploader("Upload a project plan JSON file", type=["json"])

if not uploaded:
    st.info("Upload a `.json` project plan to get started.")
    st.stop()

# Parse JSON
try:
    data = json.load(uploaded)
except json.JSONDecodeError as exc:
    st.error(f"Invalid JSON: {exc}")
    st.stop()

# Validate
errors = validate_input(data, SCHEMA_PATH)
if errors:
    st.error("Validation errors:")
    for err in errors:
        st.markdown(f"- {err}")
    st.stop()

# ── Preview tree ────────────────────────────────────────────────────────────

meta = data["metadata"]
st.subheader(f"{meta['project']} v{meta['version']}")
if meta.get("description"):
    st.caption(meta["description"])

epic_count = len(data["epics"])
feat_count = sum(len(e["features"]) for e in data["epics"])
task_count = sum(len(t["tasks"]) for e in data["epics"] for t in e["features"])
st.markdown(f"**{epic_count}** epic(s), **{feat_count}** feature(s), **{task_count}** task(s)")

for epic in data["epics"]:
    with st.expander(f"Epic: {epic['title']}", expanded=True):
        if epic.get("description"):
            st.caption(epic["description"])
        for feature in epic["features"]:
            owner = feature["ownerUserIds"][0]
            st.markdown(f"**Feature:** {feature['title']}  \n*Owner:* `{owner}`")
            if feature.get("description"):
                st.caption(feature["description"])
            for task in feature["tasks"]:
                st.markdown(f"- {task['title']}")

st.divider()

# ── Actions ─────────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)
dry_run = col1.button("Dry Run", use_container_width=True)
create = col2.button("Create Work Items", type="primary", use_container_width=True)

if not dry_run and not create:
    st.stop()


def _show_summary(summary: Summary):
    c1, c2, c3 = st.columns(3)
    c1.metric("Created", summary.created)
    c2.metric("Skipped (duplicate)", summary.skipped)
    c3.metric("Failed", summary.failed)
    if summary.failures:
        st.error("Failures:")
        for f in summary.failures:
            st.markdown(f"- {f}")


if dry_run:
    config = {"org_url": "", "project": "", "pat": ""}
    summary = process_epics(
        client=None,
        config=config,
        epics=data["epics"],
        dry_run=True,
        skip_duplicate_check=True,
    )
    st.subheader("Dry Run Results")
    _show_summary(summary)

if create:
    try:
        config = _get_config()
    except (ValueError, KeyError) as exc:
        st.error(f"Configuration error: {exc}. Check `.streamlit/secrets.toml`.")
        st.stop()

    client = AzureDevOpsClient(config["org_url"], config["project"], config["pat"])

    with st.spinner("Creating work items in Azure DevOps..."):
        try:
            summary = process_epics(
                client=client,
                config=config,
                epics=data["epics"],
                dry_run=False,
                skip_duplicate_check=False,
            )
        except AzureDevOpsError as exc:
            st.error(f"Azure DevOps API error: {exc}")
            st.stop()

    st.subheader("Results")
    _show_summary(summary)

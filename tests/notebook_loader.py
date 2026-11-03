"""
Utility for unit-testing Databricks notebook (.ipynb) source code with plain
pytest, with no live cluster, no Databricks Connect, and no extra parsing
dependency (the .ipynb format is plain JSON, so stdlib `json` is enough).

Why this exists
----------------
Our notebooks (01_data_profiling.ipynb, 01_chunk1_initial.ipynb, ...) define
reusable functions at the top of a single "Databricks notebook source" cell,
then immediately *use* those functions against a real Volume path / real
Spark session / real dbutils widgets. That's perfect for Databricks, but it
means naively exec'ing the whole cell in a unit test would try to read from
"/Volumes/vstone/..." and fail outside a real workspace.

This loader fixes that by:
  1. Reading the raw .ipynb JSON and concatenating every code cell's source.
  2. Stripping Databricks' own magic comments ("# Databricks notebook
     source", "# COMMAND ----------").
  3. Parsing the result with `ast` and keeping ONLY definitions (imports,
     functions, classes) - every top-level executable statement (variable
     assignments like VOLUME_PATH = ..., the `if __name__ == "__main__":`
     block, direct spark.read(...) calls, etc.) is dropped before exec.
  4. Executing the definitions-only module in a fresh namespace that the
     caller pre-seeds with fakes for Databricks-only globals (`spark`,
     `dbutils`, `display`).

This lets tests exercise the *actual* functions shipped in the notebook
(profile_dataset, get_parameters, create_bronze_table, ...) exactly as
written, with zero changes to the notebooks themselves.
"""
import ast
import json
from pathlib import Path
from typing import Any, Dict, Optional

_NOTEBOOK_MARKER_PREFIXES = ("# Databricks notebook source", "# COMMAND ----------")

# AST node types that are safe to keep: definitions only, never executable
# statements that would reach out to a real cluster/volume/widget.
_DEFINITION_NODE_TYPES = (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def _read_ipynb_source(ipynb_path: str) -> str:
    """Concatenate the source of every code cell in a .ipynb file (plain JSON, no nbformat needed)."""
    with open(ipynb_path, "r", encoding="utf-8") as f:
        notebook = json.load(f)

    chunks = []
    for cell in notebook.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        source = cell.get("source", "")
        text = "".join(source) if isinstance(source, list) else source
        chunks.append(text)
    return "\n\n".join(chunks)


def _strip_databricks_markers(source: str) -> str:
    """Drop Databricks' own magic comment lines; harmless to keep, but noisy in tracebacks."""
    lines = [
        line
        for line in source.splitlines()
        if not line.strip().startswith(_NOTEBOOK_MARKER_PREFIXES)
    ]
    return "\n".join(lines)


def _definitions_only(source: str) -> ast.Module:
    """Return an ast.Module containing only top-level import/function/class definitions."""
    tree = ast.parse(source)
    tree.body = [node for node in tree.body if isinstance(node, _DEFINITION_NODE_TYPES)]
    ast.fix_missing_locations(tree)
    return tree


def load_notebook_definitions(ipynb_path: str, injected_globals: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Load only the function/class/import definitions from a Databricks .ipynb
    notebook into an isolated namespace, WITHOUT running any of the notebook's
    top-level execution code (data loads, dbutils.widgets calls, main blocks).

    Args:
        ipynb_path: path to the .ipynb file under test.
        injected_globals: fakes/mocks seeded into the namespace before exec,
            e.g. {"spark": fake_spark, "dbutils": fake_dbutils, "display": lambda df: None}.
            These become the free variables the notebook's functions resolve
            against when they reference `spark`, `dbutils`, `display`, etc.

    Returns:
        The namespace dict after exec. Pull functions out of it, e.g.
        ns["profile_dataset"], ns["create_bronze_table"].
    """
    path = Path(ipynb_path)
    if not path.exists():
        raise FileNotFoundError(f"Notebook not found: {ipynb_path}")

    raw_source = _read_ipynb_source(str(path))
    clean_source = _strip_databricks_markers(raw_source)
    tree = _definitions_only(clean_source)

    namespace: Dict[str, Any] = {"__name__": f"_notebook_under_test_{path.stem}"}
    if injected_globals:
        namespace.update(injected_globals)

    code = compile(tree, filename=str(path), mode="exec")
    exec(code, namespace)
    return namespace
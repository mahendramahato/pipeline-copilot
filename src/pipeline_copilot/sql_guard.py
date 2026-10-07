"""SQL guardrail: decides whether a query may reach Athena.

Pure function, no AWS: parse the SQL into a tree with sqlglot and check its
STRUCTURE. Text checks like sql.startswith("SELECT") are easy to fool
("SELECT 1; DROP TABLE x", comments, WITH ... INSERT). A parse tree isn't.

This is layer 1 of 4. IAM, the workgroup scan limit and the row cap still
apply even if this code has a bug.
"""
import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

DEFAULT_LIMIT = 100     # added when the query has no LIMIT
MAX_LIMIT = 1000        # larger LIMITs are reduced to this

# Node types that change data or schema. Looked up by name because sqlglot
# renames classes between versions; names missing in this version are skipped.
_WRITE_NODE_NAMES = [
    "Insert", "Update", "Delete", "Merge", "Create", "Drop",
    "Alter", "AlterTable", "TruncateTable", "Command",
]
WRITE_NODES = tuple(getattr(exp, n) for n in _WRITE_NODE_NAMES if hasattr(exp, n))


class UnsafeQueryError(ValueError):
    """The query was rejected. The message says why, so the agent can fix it."""


def check_query(sql: str, database: str, allowed_tables: set[str], dialect: str = "athena") -> str:
    """Return a safe, normalized version of `sql`, or raise UnsafeQueryError."""

    # --- Rule 1: must parse ---
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except ParseError as e:
        raise UnsafeQueryError(f"Could not parse SQL: {e}") from e

    # --- Rule 2: exactly one statement ---
    # A trailing ";" produces an empty (None) statement, which we ignore.
    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise UnsafeQueryError(f"Exactly one SQL statement is allowed, got {len(statements)}.")
    stmt = statements[0]

    # --- Rule 3: the statement itself must be a query ---
    # exp.Query covers SELECT and set operations (UNION/INTERSECT/EXCEPT).
    # "WITH x AS (...) INSERT ..." parses as an Insert, so it fails here.
    if not isinstance(stmt, exp.Query):
        raise UnsafeQueryError(f"Only SELECT queries are allowed, got {stmt.key.upper()}.")

    # --- Rule 4: no write/DDL nodes anywhere in the tree ---
    if (bad := stmt.find(*WRITE_NODES)) is not None:
        raise UnsafeQueryError(f"Query contains a forbidden {bad.key.upper()} operation.")

    # --- Rule 5: only known tables, only in our database ---
    # Names defined in WITH (CTEs) look like tables in the tree; they're fine.
    cte_names = {cte.alias for cte in stmt.find_all(exp.CTE)}
    for table in stmt.find_all(exp.Table):
        if table.name in cte_names and not table.db:
            continue
        if table.catalog or (table.db and table.db != database):
            raise UnsafeQueryError(
                f"Table {table.sql()} is outside the allowed database '{database}'."
            )
        if table.name not in allowed_tables:
            raise UnsafeQueryError(
                f"Unknown table '{table.name}'. Allowed: {', '.join(sorted(allowed_tables))}."
            )

    # --- Rule 6: force a LIMIT ---
    # Keeps result sets small enough for Claude's context (and your bill).
    limit = stmt.args.get("limit")
    current = None
    if limit is not None and isinstance(limit.expression, exp.Literal) and limit.expression.is_int:
        current = int(limit.expression.this)
    if current is None or current > MAX_LIMIT:
        stmt = stmt.limit(min(current or DEFAULT_LIMIT, MAX_LIMIT))

    return stmt.sql(dialect=dialect)

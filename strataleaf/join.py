from dataclasses import dataclass
from .codec import digest, field
from .cursors import decode_cursor, encode_cursor
from .errors import InvalidCursor, InvalidDocument, NotFound
from .indexes import equality_candidates
from .mvcc import Record, checked_revision, latest_row, record, visible_rows
from .predicates import equality_terms, matches, validate_predicate


@dataclass(frozen=True)
class JoinGroup:
    """One left document with its target references resolved at the read revision."""
    record: Record
    matches: tuple
    missing: tuple
    deleted: tuple


@dataclass(frozen=True)
class JoinPage:
    groups: list
    revision: int
    cursor: str | None
    plan: dict


def reference_keys(document, path):
    value = field(document, path)
    if isinstance(value, str):
        values = (value,)
    elif isinstance(value, list):
        values = tuple(item for item in value if isinstance(item, str))
    else:
        return ()
    return tuple(sorted(set(values)))


class Joins:
    def __init__(self, store):
        self.store = store

    def lookup(self, collection, on, target, predicate=None, limit=100, cursor=None, revision=None, force_scan=False):
        field({}, on)
        validate_predicate(predicate)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("page size must be between 1 and 1000")
        with self.store.read() as connection:
            for required in (collection, target):
                if not connection.execute("SELECT 1 FROM collections WHERE name=?", (required,)).fetchone():
                    raise NotFound("unknown collection: " + required)
            fingerprint = digest({"collection": collection, "on": on, "predicate": predicate, "target": target})
            after = None
            if cursor is not None:
                payload = decode_cursor(self.store, cursor)
                if payload.get("join") != fingerprint or (revision is not None and revision != payload.get("revision")):
                    raise InvalidCursor("cursor does not belong to this join/read revision")
                revision, after = payload.get("revision"), payload.get("after")
                if not isinstance(after, str):
                    raise InvalidCursor("cursor has no key boundary")
            revision = checked_revision(self.store, revision)
            plan = {"access": "version_scan", "index": None}
            candidates = None
            if not force_scan and revision == int(self.store.get("head")):
                for path, value in equality_terms(predicate):
                    indexed = equality_candidates(connection, collection, path, value)
                    if indexed is not None:
                        index_name, candidates = indexed
                        plan = {"access": "equality_index", "index": index_name}
                        break
            if candidates is None:
                rows = visible_rows(connection, collection, revision)
            else:
                rows = [latest_row(connection, collection, key, revision) for key in sorted(candidates)]
            left = []
            examined = 0
            for row in rows:
                if row is None or row["body"] is None or (after is not None and row["key"] <= after):
                    continue
                examined += 1
                item = record(row)
                if matches(item.document, predicate):
                    left.append(item)
                    if len(left) > limit:
                        break
            continuation = None
            if len(left) > limit:
                left = left[:limit]
                continuation = encode_cursor(self.store, {"database": self.store.get("database_id"),
                    "join": fingerprint, "revision": revision, "after": left[-1].key})
            resolved = {}
            for item in left:
                for reference in reference_keys(item.document, on):
                    if reference not in resolved:
                        resolved[reference] = latest_row(connection, target, reference, revision)
            groups = []
            for item in left:
                matched, missing, deleted = [], [], []
                for reference in reference_keys(item.document, on):
                    row = resolved[reference]
                    if row is None:
                        missing.append(reference)
                    elif row["body"] is None:
                        deleted.append(reference)
                    else:
                        matched.append(record(row))
                groups.append(JoinGroup(item, tuple(matched), tuple(missing), tuple(deleted)))
            return JoinPage(groups, revision, continuation,
                            {**plan, "examined": examined, "resolved": len(resolved)})

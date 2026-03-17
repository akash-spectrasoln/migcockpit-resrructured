"""
Remap row keys from technical/db names to business names at the destination boundary.
Used only at the migration destination write path.
"""

from typing import Any, Optional


def _resolve_final_business_name(
    column_metadata: list[dict[str, Any]],
) -> tuple[dict[str, str], list[str]]:
    """
    Build source_key -> target_key mapping and ordered target column list.
    Handles duplicate business names by appending _1, _2 deterministically.

    Returns:
        (source_to_target: dict[str, str], ordered_targets: list[str])
    """
    source_to_target: dict[str, str] = {}
    business_name_count: dict[str, int] = {}
    ordered_targets: list[str] = []

    for col in column_metadata:
        business = col.get("business_name") or col.get("name") or ""
        if not business:
            business = col.get("technical_name") or col.get("db_name") or ""
        if not business:
            continue

        if business in business_name_count:
            business_name_count[business] += 1
            final_target = f"{business}_{business_name_count[business]}"
        else:
            business_name_count[business] = 1
            final_target = business

        ordered_targets.append(final_target)

        tech = col.get("technical_name")
        db = col.get("db_name")
        if tech:
            source_to_target[tech] = final_target
        if db and db not in source_to_target:
            source_to_target[db] = final_target
        if col.get("name") and col.get("name") not in source_to_target:
            source_to_target[col["name"]] = final_target
        if col.get("business_name") and col.get("business_name") not in source_to_target:
            source_to_target[col["business_name"]] = final_target

        if tech:
            if tech.endswith("_l"):
                base = tech[:-2]
                for alt in (f"__L__.{base}", f"_L_{base}"):
                    if alt not in source_to_target:
                        source_to_target[alt] = final_target
            elif tech.endswith("_r"):
                base = tech[:-2]
                for alt in (f"__R__.{base}", f"_R_{base}"):
                    if alt not in source_to_target:
                        source_to_target[alt] = final_target
            else:
                for alt in (f"__L__.{tech}", f"__R__.{tech}", f"_L_{tech}", f"_R_{tech}"):
                    if alt not in source_to_target:
                        source_to_target[alt] = final_target

    return source_to_target, ordered_targets

def remap_rows_to_business_names(
    rows: list[dict[str, Any]],
    column_metadata: Optional[list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Remap row keys from technical_name/db_name to business_name for destination persistence."""
    if not rows:
        return []
    if not column_metadata:
        return [dict(row) for row in rows]

    source_to_target, ordered_targets = _resolve_final_business_name(column_metadata)
    if not source_to_target:
        return [dict(row) for row in rows]

    target_to_sources: dict[str, list[str]] = {}
    for src, tgt in source_to_target.items():
        if tgt not in target_to_sources:
            target_to_sources[tgt] = []
        target_to_sources[tgt].append(src)

    result: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            result.append({})
            continue

        new_row: dict[str, Any] = {}
        mapped_sources: set = set()

        for target in ordered_targets:
            for src in target_to_sources.get(target, []):
                if src in row:
                    new_row[target] = row[src]
                    mapped_sources.add(src)
                    break

        for k, v in row.items():
            if k in mapped_sources:
                continue
            if k in source_to_target:
                tgt = source_to_target[k]
                if tgt not in new_row:
                    new_row[tgt] = v
            else:
                new_row[k] = v

        result.append(new_row)

    return result

def get_business_columns_from_metadata(
    column_metadata: Optional[list[dict[str, Any]]],
) -> list[str]:
    """Return ordered business column names from metadata for table schema (loaders)."""
    if not column_metadata:
        return []
    _, ordered_targets = _resolve_final_business_name(column_metadata)
    return ordered_targets

def extract_row_values_by_metadata(
    row: dict[str, Any],
    column_metadata: list[dict[str, Any]],
) -> list[Any]:
    """Extract values from a row in metadata column order; None for missing keys."""
    if not column_metadata:
        return []
    source_to_target, ordered_targets = _resolve_final_business_name(column_metadata)
    target_to_sources: dict[str, list[str]] = {}
    for src, tgt in source_to_target.items():
        if tgt not in target_to_sources:
            target_to_sources[tgt] = []
        target_to_sources[tgt].append(src)

    values: list[Any] = []
    for target in ordered_targets:
        val = row.get(target)
        if val is not None:
            values.append(val)
            continue
        for src in target_to_sources.get(target, []):
            if src in row:
                values.append(row[src])
                break
        else:
            values.append(None)
    return values

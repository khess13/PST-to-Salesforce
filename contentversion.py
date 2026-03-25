"""
generate_content_version_csv.py

Recursively scans an "output" folder and generates a ContentVersion CSV
ready for Salesforce Data Loader. Each file is base64-encoded inline and
linked to its parent EmailMessage via FirstPublishLocationId.

Expected folder structure:
    output/
      <custom_external_id>/
        attachment1.pdf
        attachment2.xlsx

Mapping sources (pick one, in priority order):
  1. --map email_id_map.csv        Two-column CSV: external_id, email_message_id
  2. --pst-emails emails.csv       PST extraction CSV — script derives the map
                                   from the uuid and salesforce_id columns

Output:
    content_version_upload.csv     Ready to load via Data Loader into ContentVersion

Usage:
    # From a pre-built map
    python generate_content_version_csv.py --map email_id_map.csv

    # Derived directly from your PST emails CSV
    python generate_content_version_csv.py --pst-emails emails.csv

    # All explicit
    python generate_content_version_csv.py \\
        --output-dir ./output \\
        --pst-emails emails.csv \\
        --out content_version_upload.csv
"""

import argparse
import base64
import csv
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_MAP_CSV    = Path("email_id_map.csv")
DEFAULT_OUT_CSV    = Path("content_version_upload.csv")

# Column names expected in the PST emails CSV
PST_UUID_COL  = "uuid"             # UUID used as the output subfolder name
PST_SFID_COL  = "salesforce_id"    # Salesforce EmailMessage Id written after upsert

# ContentVersion fields written to the output CSV
CV_FIELDS = [
    "Title",                   # display name in Salesforce (filename without extension)
    "PathOnClient",            # full filename including extension
    "VersionData",             # base64-encoded file contents
    "ContentLocation",         # always "S" (stored in Salesforce)
    "FirstPublishLocationId",  # SF EmailMessage Id — auto-creates ContentDocumentLink
    "Description",             # relative path for traceability, e.g. uuid/invoice.pdf
]


# ---------------------------------------------------------------------------
# Map loading
# ---------------------------------------------------------------------------

def load_map_from_csv(map_csv: Path) -> dict[str, str]:
    """
    Load a pre-built external_id → Salesforce EmailMessage Id map.

    Expected columns: external_id, email_message_id
    """
    if not map_csv.exists():
        print(f"[ERROR] Mapping file not found: {map_csv.resolve()}")
        sys.exit(1)

    mapping: dict[str, str] = {}
    with open(map_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"external_id", "email_message_id"}
        if not required.issubset(reader.fieldnames or []):
            print(
                f"[ERROR] '{map_csv}' must have columns: {', '.join(required)}\n"
                f"        Found: {reader.fieldnames}"
            )
            sys.exit(1)
        for row in reader:
            ext_id = row["external_id"].strip()
            sf_id  = row["email_message_id"].strip()
            if ext_id and sf_id:
                mapping[ext_id] = sf_id

    print(f"  Loaded {len(mapping)} mapping(s) from {map_csv}")
    return mapping


def load_map_from_pst_emails(pst_csv: Path) -> dict[str, str]:
    """
    Derive the external_id → Salesforce EmailMessage Id map directly from
    the PST extraction emails CSV.

    Expected columns (at minimum):
        uuid           — the UUID used as the output subfolder name
        salesforce_id  — the Salesforce EmailMessage Id populated after upsert

    Rows where salesforce_id is blank are skipped with a warning.
    """
    if not pst_csv.exists():
        print(f"[ERROR] PST emails CSV not found: {pst_csv.resolve()}")
        sys.exit(1)

    mapping: dict[str, str] = {}
    skipped = 0

    with open(pst_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        missing_cols = {PST_UUID_COL, PST_SFID_COL} - set(reader.fieldnames or [])
        if missing_cols:
            print(
                f"[ERROR] '{pst_csv}' is missing expected column(s): {missing_cols}\n"
                f"        Found: {reader.fieldnames}\n"
                f"        Update PST_UUID_COL / PST_SFID_COL at the top of the script "
                f"if your column names differ."
            )
            sys.exit(1)

        for row in reader:
            uuid  = row[PST_UUID_COL].strip()
            sf_id = row[PST_SFID_COL].strip()
            if not uuid:
                continue
            if not sf_id:
                skipped += 1
                continue
            mapping[uuid] = sf_id

    if skipped:
        print(
            f"  [WARN] {skipped} row(s) in '{pst_csv}' had no salesforce_id "
            f"and were skipped.\n"
            f"         Make sure you have upserted emails into Salesforce "
            f"and that the salesforce_id column is populated."
        )

    print(f"  Derived {len(mapping)} mapping(s) from {pst_csv}")
    return mapping


# ---------------------------------------------------------------------------
# File scanning
# ---------------------------------------------------------------------------

def encode_file(path: Path) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def scan_output_folder(
    root: Path,
    email_id_map: dict[str, str],
) -> tuple[list[dict], list[str]]:
    """
    Walk the output folder and build ContentVersion rows.

    Returns:
        rows     — list of dicts ready to write to CSV
        warnings — list of human-readable messages for skipped files
    """
    rows: list[dict] = []
    warnings: list[str] = []

    all_files = sorted(f for f in root.rglob("*") if f.is_file())

    if not all_files:
        print(f"[WARN] No files found under {root.resolve()}")
        return rows, warnings

    for file_path in all_files:
        relative    = file_path.relative_to(root)
        external_id = relative.parts[0] if len(relative.parts) > 1 else None

        if external_id is None:
            warnings.append(f"SKIP (root-level, no folder key): {file_path.name}")
            continue

        sf_id = email_id_map.get(external_id)
        if not sf_id:
            warnings.append(
                f"SKIP (no SF Id mapped for '{external_id}'): {relative}"
            )
            continue

        rows.append({
            "Title":                  file_path.stem,
            "PathOnClient":           file_path.name,
            "VersionData":            encode_file(file_path),
            "ContentLocation":        "S",
            "FirstPublishLocationId": sf_id,
            "Description":            str(relative),
        })

    return rows, warnings


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_csv(rows: list[dict], out_path: Path) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Wrote {len(rows)} row(s) → {out_path.resolve()}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a ContentVersion CSV for Salesforce Data Loader.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Mapping source (choose one):
  --map           Pre-built CSV with columns: external_id, email_message_id
  --pst-emails    PST extraction emails CSV; map is derived from uuid + salesforce_id columns

If both are provided, --pst-emails takes priority.
If neither is provided, the script looks for email_id_map.csv in the current directory.
        """,
    )
    parser.add_argument(
        "--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR,
        help=f"Root folder to scan for attachments (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--map", type=Path, default=None,
        help="Pre-built mapping CSV (external_id → email_message_id)",
    )
    parser.add_argument(
        "--pst-emails", type=Path, default=None,
        metavar="PST_EMAILS_CSV",
        help="PST extraction emails CSV to derive the mapping from",
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT_CSV,
        help=f"Output CSV path (default: {DEFAULT_OUT_CSV})",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()

    if not args.output_dir.exists():
        print(f"[ERROR] Output folder not found: {args.output_dir.resolve()}")
        sys.exit(1)

    # Resolve mapping source
    if args.pst_emails:
        map_source = f"PST emails CSV ({args.pst_emails})"
        email_id_map = load_map_from_pst_emails(args.pst_emails)
    elif args.map:
        map_source = f"map CSV ({args.map})"
        email_id_map = load_map_from_csv(args.map)
    elif DEFAULT_MAP_CSV.exists():
        map_source = f"map CSV ({DEFAULT_MAP_CSV}) [default]"
        email_id_map = load_map_from_csv(DEFAULT_MAP_CSV)
    else:
        print(
            "[ERROR] No mapping source found.\n"
            "        Provide --map <file> or --pst-emails <file>.\n"
            "        Run with --help for details."
        )
        sys.exit(1)

    print(f"\nScanning    : {args.output_dir.resolve()}")
    print(f"Map source  : {map_source}")
    print(f"Output CSV  : {args.out.resolve()}\n")

    rows, warnings = scan_output_folder(args.output_dir, email_id_map)

    if warnings:
        print(f"── Warnings ({len(warnings)}) ──")
        for w in warnings:
            print(f"  {w}")
        print()

    if not rows:
        print("[ERROR] No rows to write. Check your folder structure and mapping source.")
        sys.exit(1)

    print("── Writing CSV ──")
    write_csv(rows, args.out)

    print(f"""
┌─────────────────────────────────┐
│            Summary              │
├──────────────────┬──────────────┤
│  Files included  │ {len(rows):>12} │
│  Files skipped   │ {len(warnings):>12} │
└──────────────────┴──────────────┘
""")


if __name__ == "__main__":
    main()

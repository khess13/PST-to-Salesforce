"""
PST to Salesforce CSV Extractor  (Standard EmailMessage Object)
===============================================================
Extracts emails and attachments from an Outlook .pst file and exports
them into multiple relational CSV files ready for Salesforce import
using the STANDARD Salesforce objects.

Output Tables (CSV files) — load in this order:
  1. emails.csv                → EmailMessage  (Insert)
  2. email_relations.csv       → EmailMessageRelation  (Insert)
  3. content_versions.csv      → ContentVersion  (Insert — auto-creates ContentDocument)
  4. content_document_links.csv → ContentDocumentLink  (Insert)
  5. email_status_update.csv   → EmailMessage  (Update Status to 3=Sent)

  attachment_files/            → Raw attachment binaries (--save-attachments flag)

⚠️  IMPORTANT LOADING RULES (Salesforce quirks):
  - DO NOT set Status=3 (Sent) during initial EmailMessage insert — it locks the
    record and blocks all child inserts. Load status last (step 5).
  - DO NOT set CreatedById unless IsClientManaged=TRUE, or only the original
    user can delete the record (even admins cannot).
  - Set IsClientManaged=TRUE to bypass both of the above restrictions.
  - EmailMessageRelation has NO external ID field — use Insert (not Upsert).
  - For ContentVersion, set FirstPublishLocationId = EmailMessage.Id to
    automatically create the ContentDocumentLink (skips step 4).

Requirements:
    pip install libpff-python pandas tqdm

Usage:
    python pst_to_salesforce.py --pst path/to/file.pst --out ./output
    python pst_to_salesforce.py --pst file.pst --out ./output --save-attachments
"""

import argparse
import csv
import hashlib
import logging
import os
import re
import sys
import uuid
from datetime import timezone
from pathlib import Path

import pandas as pd

try:
    import pypff  # libpff-python
except ImportError:
    sys.exit(
        "ERROR: 'libpff-python' is not installed.\n"
        "Install it with:  pip install libpff-python"
    )

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_str(value) -> str:
    """Return a clean UTF-8 string; never raises.
    Handles bytes returned by pypff (plain_text_body, html_body, sender_name etc.)
    by decoding with UTF-8 first, falling back to cp1252, then latin-1.
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        for enc in ("utf-8", "cp1252", "latin-1"):
            try:
                return value.decode(enc).strip()
            except (UnicodeDecodeError, AttributeError):
                continue
        return value.decode("latin-1", errors="replace").strip()
    try:
        return str(value).strip()
    except Exception:
        return ""


def _safe_dt(dt_obj) -> str:
    """Convert a pypff datetime to an ISO-8601 string (UTC)."""
    if dt_obj is None:
        return ""
    try:
        if hasattr(dt_obj, "astimezone"):
            return dt_obj.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if hasattr(dt_obj, "strftime"):
            return dt_obj.strftime("%Y-%m-%dT%H:%M:%SZ")
        return str(dt_obj)
    except Exception:
        return ""


# Matches RTF headers that pypff sometimes returns in plain_text_body
_RTF_HEADER_RE  = re.compile(r'^\s*\{\\rtf', re.IGNORECASE)
# Matches XML/HTML-like attribute noise: Locked="false" Priority="49" etc.
_XML_ATTR_RE    = re.compile(r'\b\w+\s*=\s*"[^"]{0,200}"')


def _clean_body(text: str) -> str:
    """
    Sanitise email body text for safe CSV output.
    - Removes null bytes and non-printable control characters (except tab/LF/CR)
    - Detects and rejects RTF payloads that pypff returns instead of plain text
    - Collapses excessive blank lines
    """
    if not text:
        return ""

    # Null bytes and non-printable control chars break CSV parsers
    text = text.replace("\x00", "")
    text = re.sub(r"[\x01-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)

    # pypff occasionally returns RTF markup in plain_text_body when no plain
    # text part exists — these look like:  {\rtf1\ansi ... Locked="false" ...}
    # Returning this as TextBody would pollute every downstream field.
    if _RTF_HEADER_RE.match(text):
        return ""   # caller should fall back to html_body or leave blank

    # Collapse runs of blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


_WINDOWS_RESERVED = re.compile(
    r'^(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(\.|$)', re.IGNORECASE
)


def _sanitise_filename(name: str, max_len: int = 200) -> str:
    """Remove illegal path characters, handle OS reserved names, and truncate
    the stem to stay within max_len — always preserving the file extension."""
    name = os.path.basename(name)
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    if _WINDOWS_RESERVED.match(name):
        name = f"_{name}"
    name = name or "attachment"

    if len(name) > max_len:
        # Split on the LAST dot so  "report.final.xlsx" -> stem="report.final", ext=".xlsx"
        dot = name.rfind(".")
        if dot > 0:
            stem, ext = name[:dot], name[dot:]
            ext = ext[:20]                          # cap extension length too
            stem = stem[: max_len - len(ext)]
            name = stem + ext
        else:
            name = name[:max_len]

    return name


# Matches the address in  "Display Name <addr@example.com>"  or bare  addr@example.com
_TRANSPORT_FROM_RE = re.compile(
    r'^From:\s*(?:.*?<([^>]+)>|(\S+@\S+))', re.IGNORECASE | re.MULTILINE
)


def _parse_sender_email(message) -> str:
    """
    Extract the sender's email address from transport_headers.
    pypff does not expose sender_email_address directly; the From header
    inside transport_headers is the reliable source.

    Falls back to an empty string if headers are absent or unparseable.
    """
    try:
        # Use get_transport_headers() — the confirmed pypff getter name.
        # Returns bytes; _safe_str decodes them.
        raw = message.get_transport_headers()
        headers = _safe_str(raw) if raw is not None else ""
    except Exception:
        return ""

    if not headers:
        return ""

    m = _TRANSPORT_FROM_RE.search(headers)
    if m:
        # Group 1 = address inside angle-brackets, group 2 = bare address
        return (m.group(1) or m.group(2) or "").strip().lower()

    return ""


# ---------------------------------------------------------------------------
# Core extraction
# ---------------------------------------------------------------------------

class PSTExtractor:
    """Walk a PST file and collect emails, recipients and attachments."""

    def __init__(self, pst_path: str, save_attachments: bool = False, attachment_dir: Path = None):
        self.pst_path = pst_path
        self.save_attachments = save_attachments
        self.attachment_dir = attachment_dir

        # Rows for each CSV table
        self.emails: list[dict] = []
        self.recipients: list[dict] = []
        self.attachments: list[dict] = []

        self._email_count = 0

    # ------------------------------------------------------------------
    def extract(self):
        log.info("Opening PST: %s", self.pst_path)
        pst = pypff.file()
        pst.open(self.pst_path)
        root = pst.get_root_folder()
        self._walk_folder(root, folder_path="")
        pst.close()
        log.info(
            "Extraction complete — %d emails, %d recipients, %d attachments",
            len(self.emails),
            len(self.recipients),
            len(self.attachments),
        )

    # ------------------------------------------------------------------
    def _walk_folder(self, folder, folder_path: str):
        """Recursively walk PST folders."""
        try:
            folder_name = _safe_str(folder.name) or "Root"
        except Exception:
            folder_name = "Unknown"

        current_path = f"{folder_path}/{folder_name}".lstrip("/")

        # Process messages in this folder
        num_messages = folder.number_of_sub_messages
        iterator = range(num_messages)
        if HAS_TQDM and num_messages > 0:
            iterator = tqdm(iterator, desc=f"📂 {current_path[:60]}", unit="msg", leave=False)

        for i in iterator:
            try:
                message = folder.get_sub_message(i)
                self._process_message(message, folder_path=current_path)
            except Exception as exc:
                log.warning("Skipping message %d in '%s': %s", i, current_path, exc)

        # Recurse into sub-folders
        for j in range(folder.number_of_sub_folders):
            try:
                sub_folder = folder.get_sub_folder(j)
                self._walk_folder(sub_folder, folder_path=current_path)
            except Exception as exc:
                log.warning("Skipping sub-folder %d in '%s': %s", j, current_path, exc)

    # ------------------------------------------------------------------
    def _process_message(self, message, folder_path: str):
        email_id = str(uuid.uuid4())
        self._email_count += 1

        # ---- Core fields ------------------------------------------------
        # Use explicit get_*() methods — confirmed by pypff C source at
        # github.com/libyal/libpff. Property access silently returns None
        # on many pypff builds; getters raise AttributeError if unavailable,
        # which _safe_call() converts to "".
        def _get(fn, *args):
            """Call a pypff getter safely; return '' on any error."""
            try:
                val = fn(*args)
                return _safe_str(val)
            except Exception:
                return ""

        def _get_dt(fn):
            try:
                return _safe_dt(fn())
            except Exception:
                return ""

        subject      = _get(message.get_subject)
        sender       = _get(message.get_sender_name)
        sender_email = _parse_sender_email(message)

        # Body getters return bytes — _safe_str decodes them.
        # plain_text_body may be RTF; _clean_body returns "" in that case.
        body_plain = _clean_body(_get(message.get_plain_text_body))
        try:
            body_html = _clean_body(_get(message.get_html_body))
        except OSError:
            body_html = ""

        # client_submit_time = when sender sent; delivery_time = when received.
        sent_dt = (
            _get_dt(message.get_client_submit_time)
            or _get_dt(message.get_delivery_time)
        )
        has_attach = message.number_of_attachments > 0

        self.emails.append({
            "Id":          email_id,      # internal surrogate key
            "Subject":     subject,
            "SenderName":  sender,
            "SenderEmail": sender_email,
            "SentDate":    sent_dt,
            "BodyPlain":   body_plain,
            "BodyHtml":    body_html,
            "FolderPath":  folder_path,
        })

        # ---- Recipients -------------------------------------------------
        self._extract_recipients(message, email_id)

        # ---- Attachments ------------------------------------------------
        if has_attach:
            self._extract_attachments(message, email_id)

    # ------------------------------------------------------------------
    def _extract_recipients(self, message, email_id: str):
        """Parse To / CC / BCC recipient headers."""
        # pypff exposes recipients via the recipients collection
        try:
            num_recip = message.number_of_recipients
        except Exception:
            num_recip = 0

        for i in range(num_recip):
            try:
                recip = message.get_recipient(i)
                recip_type_raw = getattr(recip, "recipient_type", 0) or 0
                # 0=To, 1=CC, 2=BCC (MAPI values)
                type_map = {0: "To", 1: "CC", 2: "BCC"}
                recip_type = type_map.get(int(recip_type_raw), "To")

                def _rget(fn):
                    try:
                        return _safe_str(fn())
                    except Exception:
                        return ""
                self.recipients.append({
                    "Id":            str(uuid.uuid4()),
                    "EmailId":       email_id,
                    "RecipientType": recip_type,
                    "DisplayName":   _rget(recip.get_name),
                    "EmailAddress":  _rget(recip.get_email_address),
                })
            except Exception as exc:
                log.debug("Could not parse recipient %d: %s", i, exc)

    # ------------------------------------------------------------------
    def _extract_attachments(self, message, email_id: str):
        """Extract attachment metadata (and optionally save binaries).

        MAPI attachment method values:
          0  ATTACH_BY_VALUE       — binary data stored in PST, readable via read_buffer
          1  ATTACH_BY_REFERENCE   — file path reference only, no binary in PST
          2  ATTACH_BY_REF_RESOLVE — reference that may resolve to binary
          4  ATTACH_EMBEDDED_MSG   — embedded Outlook message (.msg inside .msg)
          6  ATTACH_OLE            — OLE object (not a plain file)
        Only method 0 and 2 are reliably readable as raw bytes.
        """
        for i in range(message.number_of_attachments):
            attach_id = str(uuid.uuid4())
            filename  = f"attachment_{i}"   # safe default before we know the real name
            try:
                attach = message.get_attachment(i)

                # ---- Filename -------------------------------------------
                # Try long filename first (PR_ATTACH_LONG_FILENAME), then
                # short 8.3 name (PR_ATTACH_FILENAME / attach.name).
                raw_name = ""
                for getter in ("get_long_filename", "get_name"):
                    try:
                        val = getattr(attach, getter)()
                        raw_name = _safe_str(val)
                        if raw_name:
                            break
                    except Exception:
                        continue
                filename = _sanitise_filename(raw_name) if raw_name else f"attachment_{i}"

                # ---- Attachment method -----------------------------------
                try:
                    attach_method = int(attach.get_attachment_method() or 0)
                except Exception:
                    attach_method = 0

                ATTACH_BY_VALUE       = 0
                ATTACH_BY_REF_RESOLVE = 2
                ATTACH_EMBEDDED_MSG   = 4
                ATTACH_OLE            = 6

                if attach_method == ATTACH_BY_VALUE or attach_method == ATTACH_BY_REF_RESOLVE:
                    attach_type = "file"
                elif attach_method == ATTACH_EMBEDDED_MSG:
                    attach_type = "embedded_msg"
                    # Give embedded messages an .msg extension for clarity
                    if not filename.lower().endswith(".msg"):
                        filename = filename + ".msg" if raw_name else f"embedded_{i}.msg"
                elif attach_method == ATTACH_OLE:
                    attach_type = "ole"
                    filename = filename or f"ole_object_{i}.bin"
                else:
                    attach_type = f"unknown_method_{attach_method}"

                # ---- Read binary data -----------------------------------
                data       = None
                size_bytes = 0
                sha256     = ""

                if attach_method in (ATTACH_BY_VALUE, ATTACH_BY_REF_RESOLVE,
                                     ATTACH_EMBEDDED_MSG, ATTACH_OLE):
                    try:
                        # Prefer read_buffer with no argument (reads all) where
                        # supported; fall back to attach.size as the length hint.
                        try:
                            attach_size = attach.get_size()
                            data = attach.read_buffer(attach_size)
                        except TypeError:
                            # Some pypff builds accept no argument
                            data = attach.read_buffer()

                        if data:
                            size_bytes = len(data)
                            sha256     = _sha256(data)
                        else:
                            log.debug(
                                "Attachment %d ('%s') on email %s returned empty buffer "
                                "(method=%d)", i, filename, email_id, attach_method
                            )
                    except Exception as read_exc:
                        log.warning(
                            "Could not read buffer for attachment %d ('%s') on email %s "
                            "(method=%d): %s", i, filename, email_id, attach_method, read_exc
                        )
                else:
                    log.info(
                        "Skipping attachment %d ('%s') on email %s — "
                        "unsupported attachment method %d",
                        i, filename, email_id, attach_method
                    )

                # ---- Optionally save to disk ----------------------------
                saved_path = ""
                if self.save_attachments and data and self.attachment_dir:
                    # Use first 8 chars of uuid as subfolder — saves 28 chars vs full uuid
                    short_id  = email_id[:8]
                    dest_dir  = self.attachment_dir / short_id
                    dest_dir.mkdir(parents=True, exist_ok=True)

                    # Calculate available filename length using the ABSOLUTE resolved
                    # path — relative paths give wrong lengths on Windows.
                    MAX_PATH   = 259
                    available  = MAX_PATH - len(str(dest_dir.resolve())) - 1
                    available  = max(available, 20)
                    safe_name  = _sanitise_filename(filename, max_len=available)

                    dest_file  = dest_dir / safe_name
                    if dest_file.exists():
                        # Collision: prefix with attachment index, re-truncate if needed
                        prefix    = f"{i}_"
                        safe_name = _sanitise_filename(filename, max_len=available - len(prefix))
                        dest_file = dest_dir / f"{prefix}{safe_name}"

                    try:
                        dest_file.write_bytes(data)
                        saved_path = str(dest_file)
                        if safe_name != filename:
                            log.debug(
                                "Filename truncated: '%s' -> '%s'", filename, safe_name
                            )
                    except OSError as write_exc:
                        log.warning(
                            "Could not save attachment '%s' to disk: %s",
                            dest_file, write_exc
                        )

                # ---- Collect metadata -----------------------------------
                mime_type  = _safe_str(getattr(attach, "mime_type", ""))
                content_id = _safe_str(getattr(attach, "content_identifier", ""))

                self.attachments.append({
                    "Id":            attach_id,
                    "EmailId":       email_id,
                    "FileName":      filename,
                    "AttachType":    attach_type,
                    "MimeType":      mime_type,
                    "SizeBytes":     size_bytes,
                    "SHA256":        sha256,
                    "ContentId":     content_id,
                    "SavedFilePath": saved_path,
                })

            except Exception as exc:
                log.warning(
                    "Could not extract attachment %d ('%s') on email %s: %s",
                    i, filename, email_id, exc
                )


# ---------------------------------------------------------------------------
# CSV export  —  Standard Salesforce object field mappings
# ---------------------------------------------------------------------------
#
# EmailMessage  (standard object)
# --------------------------------
# ExternalId__c  →  a custom External ID field YOU must create on EmailMessage
#                   in your org (Text, Unique, ExternalId=true).
# Status is intentionally OMITTED here — set it in a separate update CSV
# after all child records are loaded (see email_status_update.csv).
#
SF_EMAIL_FIELDS = {
    "Id":          "ExternalId__c",   # custom ext-id field you create in your org
    "Subject":     "Subject",
    "SenderName":  "FromName",
    "SenderEmail": "FromAddress",
    "SentDate":    "MessageDate",
    "BodyPlain":   "TextBody",
    "BodyHtml":    "HtmlBody",
    # ToAddress / CcAddress / BccAddress are simple semicolon-delimited strings
    # on EmailMessage — populated from the recipients list at export time.
    "ToAddress":   "ToAddress",
    "CcAddress":   "CcAddress",
    "BccAddress":  "BccAddress",
    # IsClientManaged=TRUE bypasses the Status lock and CreatedById restriction.
    "IsClientManaged": "IsClientManaged",
    # Helpful for traceability — store the original PST folder path.
    "FolderPath":  "Description",     # repurpose Description, or omit if unused
}

# EmailMessageRelation  (standard junction object — NOT customisable, no ext-id)
# Load with Insert (not Upsert).
# EmailMessageId must be the real Salesforce Id returned after EmailMessage insert.
# RelationType picklist: ToAddress | CcAddress | BccAddress | FromAddress | OtherAddress
SF_EMAIL_RELATION_FIELDS = {
    "EmailMessageId": "EmailMessageId",  # SF Id from step-1 result file
    "RelationType":   "RelationType",
    "RelationAddress":"RelationAddress",
    # ContactId / LeadId / UserId — leave blank if you don't have SF person IDs yet;
    # Salesforce will attempt a lookup by RelationAddress.
    "ContactId":      "RelationId",      # set to matched Contact/Lead/User SF Id
}

# ContentVersion  (stores the actual attachment binary)
# Salesforce auto-creates ContentDocument when you insert ContentVersion.
# Set FirstPublishLocationId = EmailMessage SF Id to auto-link (skips ContentDocumentLink).
SF_CONTENT_VERSION_FIELDS = {
    "Id":                      "ExternalId__c",       # custom ext-id on ContentVersion
    "EmailSfId":               "FirstPublishLocationId",  # EmailMessage SF Id (from step-1)
    "FileName":                "Title",
    "FileName":                "PathOnClient",         # must match Title for Data Loader
    "MimeType":                "VersionDataUrl",       # see note below *
    "SizeBytes":               "ContentSize",
    "SHA256":                  "Checksum",
    # VersionData (the actual binary) cannot be set via CSV — use Data Loader binary upload
    # or Salesforce Bulk API with base64-encoded body.
}

# ContentDocumentLink  (only needed if NOT using FirstPublishLocationId above)
# LinkedEntityId = EmailMessage SF Id, ContentDocumentId = from ContentVersion query
SF_CONTENT_DOC_LINK_FIELDS = {
    "ContentDocumentId": "ContentDocumentId",  # from post-insert ContentVersion query
    "LinkedEntityId":    "LinkedEntityId",     # EmailMessage SF Id
    "ShareType":         "ShareType",          # must be "V" (View) for EmailMessage
    "Visibility":        "Visibility",         # "AllUsers"
}


def write_csv(rows: list[dict], columns: list[str], out_path: Path, rename: dict = None):
    """Write rows to CSV, optionally renaming columns.

    `columns` must be the SOURCE (pre-rename) key names.  Only those columns
    are kept, in that order, before any rename is applied.  This prevents
    orphan internal fields from bleeding into the output under a wrong header.
    """
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=columns)

    # 1. Select and ORDER by source columns — drops any internal keys not in
    #    the explicit list (e.g. MessageId, Importance, HasAttachments).
    src_cols = [c for c in columns if c in df.columns]
    df = df[src_cols]

    # 2. Rename to Salesforce API names AFTER the column set is locked.
    if rename:
        df = df.rename(columns={k: v for k, v in rename.items() if k in src_cols})

    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].map({True: "TRUE", False: "FALSE"})
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL)
    log.info("  ✔ Written %d rows → %s", len(df), out_path)


def build_address_columns(recipients: list[dict]) -> dict[str, dict]:
    """
    Collapse per-row recipients into ToAddress/CcAddress/BccAddress strings
    (semicolon-delimited) keyed by email_id.
    Salesforce EmailMessage stores these as flat fields, not child rows.
    The EmailMessageRelation rows are exported separately for person linking.
    """
    addr: dict[str, dict] = {}
    type_map = {"To": "ToAddress", "CC": "CcAddress", "BCC": "BccAddress"}
    for r in recipients:
        eid = r["EmailId"]
        if eid not in addr:
            addr[eid] = {"ToAddress": [], "CcAddress": [], "BccAddress": []}
        col = type_map.get(r["RecipientType"], "ToAddress")
        addr[eid][col].append(r["EmailAddress"])
    return {
        eid: {k: ";".join(v) for k, v in cols.items()}
        for eid, cols in addr.items()
    }


# ---------------------------------------------------------------------------
# Diagnose helper
# ---------------------------------------------------------------------------

def _run_diagnose(pst_path: str):
    """Open the PST, find the first message, and print every pypff getter
    result so you can verify which attributes work on your specific build."""
    import pypff
    print("\n=== DIAGNOSE MODE ===")
    print(f"PST: {pst_path}")
    pst = pypff.file()
    pst.open(pst_path)
    root = pst.get_root_folder()

    def find_first(folder, depth=0):
        for i in range(folder.number_of_sub_messages):
            return folder.get_sub_message(i)
        for j in range(folder.number_of_sub_folders):
            msg = find_first(folder.get_sub_folder(j), depth+1)
            if msg:
                return msg

    msg = find_first(root)
    if not msg:
        print("No messages found in PST.")
        pst.close()
        return

    getters = [
        "get_subject", "get_sender_name", "get_conversation_topic",
        "get_plain_text_body", "get_html_body", "get_rtf_body",
        "get_transport_headers", "get_client_submit_time", "get_delivery_time",
        "get_message_identifier",
    ]
    print("\n--- Message getters ---")
    for g in getters:
        try:
            val = getattr(msg, g)()
            if isinstance(val, bytes):
                preview = repr(val[:80])
                print(f"  {g}() -> bytes: {preview}")
            else:
                print(f"  {g}() -> {type(val).__name__}: {repr(str(val)[:120])}")
        except AttributeError:
            print(f"  {g}() -> ATTRIBUTE MISSING")
        except Exception as e:
            print(f"  {g}() -> ERROR: {e}")

    print("\n--- Properties (for comparison) ---")
    for prop in ["subject", "sender_name", "plain_text_body", "html_body",
                 "transport_headers", "delivery_time"]:
        try:
            val = getattr(msg, prop)
            print(f"  .{prop} -> {type(val).__name__}: {repr(str(val)[:80]) if val else None}")
        except AttributeError:
            print(f"  .{prop} -> ATTRIBUTE MISSING")
        except Exception as e:
            print(f"  .{prop} -> ERROR: {e}")

    if msg.number_of_recipients > 0:
        print("\n--- First recipient ---")
        r = msg.get_recipient(0)
        for g in ["get_name", "get_email_address", "get_recipient_type"]:
            try:
                val = getattr(r, g)()
                print(f"  {g}() -> {repr(_safe_str(val))}")
            except Exception as e:
                print(f"  {g}() -> ERROR: {e}")

    pst.close()
    print("\n=== END DIAGNOSE ===\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract emails from a PST and export standard Salesforce object CSVs."
    )
    parser.add_argument("--pst",  required=True, help="Path to the .pst file")
    parser.add_argument("--out",  default="./sf_output", help="Output directory (default: ./sf_output)")
    parser.add_argument("--clean", action="store_true",
                        help="Delete any existing CSV files in --out before writing "
                             "(prevents stale column layouts from previous runs)")
    parser.add_argument("--diagnose", action="store_true",
                        help="Print raw pypff attribute values from the first message "
                             "and exit — use this to verify pypff works on your PST "
                             "before running a full extraction")
    parser.add_argument(
        "--save-attachments", action="store_true",
        help="Save raw attachment binaries to disk inside <out>/attachment_files/",
    )
    parser.add_argument(
        "--no-body-html", action="store_true",
        help="Omit HtmlBody from EmailMessage CSV (reduces file size)",
    )
    args = parser.parse_args()

    pst_path = Path(args.pst)
    if not pst_path.exists():
        sys.exit(f"ERROR: PST file not found: {pst_path}")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.clean:
        for stale in out_dir.glob("*.csv"):
            stale.unlink()
            log.info("Removed stale file: %s", stale)

    attach_dir = None
    if args.save_attachments:
        attach_dir = (out_dir / "attachment_files").resolve()  # always absolute
        attach_dir.mkdir(parents=True, exist_ok=True)

    # ---- Diagnose mode -------------------------------------------------
    if args.diagnose:
        _run_diagnose(str(pst_path))
        return

    # ---- Extract --------------------------------------------------------
    extractor = PSTExtractor(
        pst_path=str(pst_path),
        save_attachments=args.save_attachments,
        attachment_dir=attach_dir,
    )
    extractor.extract()

    # ---- Collapse recipients into ToAddress/CcAddress/BccAddress --------
    addr_by_email = build_address_columns(extractor.recipients)
    for email in extractor.emails:
        addrs = addr_by_email.get(email["Id"], {})
        email["ToAddress"]  = addrs.get("ToAddress", "")
        email["CcAddress"]  = addrs.get("CcAddress", "")
        email["BccAddress"] = addrs.get("BccAddress", "")
        email["IsClientManaged"] = True   # avoids Status lock & CreatedBy restriction

    # ---- 1. emails.csv  →  EmailMessage (Insert) ------------------------
    # Build column map — BodyHtml included BEFORE list() is called so
    # the columns arg and rename dict are always in sync.
    email_col_map = {
        "Id":              "ExternalId__c",
        "Subject":         "Subject",
        "SenderName":      "FromName",
        "SenderEmail":     "FromAddress",
        "SentDate":        "MessageDate",
        "BodyPlain":       "TextBody",
        "BodyHtml":        "HtmlBody",     # filtered out below if --no-body-html
        "ToAddress":       "ToAddress",
        "CcAddress":       "CcAddress",
        "BccAddress":      "BccAddress",
        "IsClientManaged": "IsClientManaged",
        "FolderPath":      "Description",
    }
    if args.no_body_html:
        del email_col_map["BodyHtml"]

    # columns and rename are derived from the same dict — always in sync
    email_src_cols = list(email_col_map.keys())
    write_csv(extractor.emails, email_src_cols,
              out_dir / "1_emails.csv", rename=email_col_map)

    # ---- 2. email_relations.csv  →  EmailMessageRelation (Insert) -------
    # EmailMessageId here is ExternalId__c — after inserting emails, replace
    # with real Salesforce Ids using the Data Loader result file.
    type_to_relation = {"To": "ToAddress", "CC": "CcAddress", "BCC": "BccAddress"}
    relation_rows = []
    for r in extractor.recipients:
        relation_rows.append({
            "EmailMessageId":  r["EmailId"],        # replace with real SF Id post-insert
            "RelationType":    type_to_relation.get(r["RecipientType"], "ToAddress"),
            "RelationAddress": r["EmailAddress"],
            "DisplayName":     r["DisplayName"],
            "RelationId":      "",  # fill with Contact/Lead/User SF Id if known
        })
    write_csv(
        relation_rows,
        ["EmailMessageId", "RelationType", "RelationAddress", "DisplayName", "RelationId"],
        out_dir / "2_email_relations.csv",
    )

    # ---- 3. content_versions.csv  →  ContentVersion (Insert) -----------
    # VersionData (binary) cannot go in CSV — load binaries separately via
    # Data Loader "Insert ContentVersion" with the attachment_files/ folder.
    # FirstPublishLocationId = EmailMessage SF Id auto-creates ContentDocumentLink.
    cv_rows = []
    for a in extractor.attachments:
        cv_rows.append({
            "ExternalId__c":           a["Id"],
            "FirstPublishLocationId":  a["EmailId"],   # replace with real SF Id post-insert
            "Title":                   a["FileName"],
            "PathOnClient":            a["FileName"],
            "ContentSize":             a["SizeBytes"],
            "Checksum":                a["SHA256"],
            "OriginalMimeType":        a["MimeType"],
            "SavedFilePath":           a["SavedFilePath"],  # local path for binary upload
        })
    write_csv(
        cv_rows,
        ["ExternalId__c", "FirstPublishLocationId", "Title", "PathOnClient",
         "ContentSize", "Checksum", "OriginalMimeType", "SavedFilePath"],
        out_dir / "3_content_versions.csv",
    )

    # ---- 4. email_status_update.csv  →  EmailMessage (Update) ----------
    # Load LAST. Setting Status=3 (Sent) makes the record read-only.
    # Use the Data Loader result file from step 1 to get real SF Ids.
    status_rows = [{"ExternalId__c": e["ExternalId__c"] if "ExternalId__c" in e else e["Id"],
                    "Status": "3"} for e in extractor.emails]
    # Rebuild from the already-renamed emails if needed
    status_rows = [{"ExternalId__c": e["Id"], "Status": "3"} for e in extractor.emails]
    write_csv(
        status_rows,
        ["ExternalId__c", "Status"],
        out_dir / "4_email_status_update.csv",
    )

    # ---- Summary --------------------------------------------------------
    print("\n" + "="*65)
    print("  PST → Salesforce Export Summary  (Standard Objects)")
    print("="*65)
    print(f"  PST file   : {pst_path}")
    print(f"  Output dir : {out_dir.resolve()}")
    print(f"  Emails     : {len(extractor.emails):,}")
    print(f"  Recipients : {len(extractor.recipients):,}")
    print(f"  Attachments: {len(extractor.attachments):,}")
    if args.save_attachments:
        print(f"  Attachment files saved to: {attach_dir}")
    print()
    print("  ── Load Order ──────────────────────────────────────────────")
    print("  1. 1_emails.csv              → EmailMessage      (Insert)")
    print("     ↳ Update EmailMessageId in files 2 & 3 using result CSV")
    print("  2. 2_email_relations.csv     → EmailMessageRelation (Insert)")
    print("  3. 3_content_versions.csv    → ContentVersion    (Insert)")
    print("     ↳ Upload binaries from attachment_files/ via Data Loader")
    print("  4. 4_email_status_update.csv → EmailMessage      (Update)")
    print("     ↳ Sets Status=3 (Sent) — locks records read-only")
    print()
    print("  ── Key Salesforce Notes ────────────────────────────────────")
    print("  • IsClientManaged=TRUE is set on all emails (avoids lock issues)")
    print("  • Create ExternalId__c (Text, Unique, External ID) on")
    print("    EmailMessage and ContentVersion in your org before loading")
    print("  • EmailMessageRelation has no External ID — use Insert only")
    print("  • ContentVersion.FirstPublishLocationId auto-creates the")
    print("    ContentDocumentLink — no separate step needed")
    print("  • Use Salesforce Data Loader (not Data Import Wizard) for")
    print("    EmailMessage — the wizard doesn't support this object")
    print("="*65 + "\n")


if __name__ == "__main__":
    main()

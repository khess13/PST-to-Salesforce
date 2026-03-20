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

# Matches cp1252/latin-1 range character sequences that are
# valid UTF-8 multibyte sequences misread with the wrong codec.
# Covers 2-byte (Ã©), 3-byte (â€™), and BOM (ï»¿) patterns.
_MOJIBAKE_RE = re.compile(
    r'[\xc0-\xff][\x80-\xbf]+'
    r'|ï»¿'
    r'|Â[\x80-\xbf]'
    r'|Ã[\x80-\xbf\xa0-\xbf]'
    r'|â€[\x80-\xbf\x9c-\x9f™œ]'
)


def _fix_mojibake(text: str) -> str:
    """Fix mojibake sequences individually without touching genuine Unicode.

    Operates match-by-match so that strings containing a mix of real Unicode
    characters (emoji, CJK, etc.) and mojibake sequences are handled correctly.
    The whole-string encode approach fails on mixed content because genuine
    Unicode chars above cp1252 range prevent re-encoding the entire string.
    """
    if not _MOJIBAKE_RE.search(text):
        return text

    def _replace(m: re.Match) -> str:
        seq = m.group(0)
        for enc in ('cp1252', 'latin-1'):
            try:
                return seq.encode(enc).decode('utf-8')
            except (UnicodeDecodeError, UnicodeEncodeError):
                continue
        return seq

    return _MOJIBAKE_RE.sub(_replace, text)



def _safe_str(value) -> str:
    """Return a clean Unicode string; never raises.
    - bytes: decoded via utf-8-sig -> utf-8 -> cp1252 -> latin-1
    - str:   checked for mojibake (UTF-8 decoded as latin-1) and repaired
    """
    if value is None:
        return ""
    if isinstance(value, bytes):
        for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
            try:
                result = value.decode(enc).strip()
                if _MOJIBAKE_RE.search(result):
                    result = _fix_mojibake(result)
                return result
            except (UnicodeDecodeError, AttributeError):
                continue
        return value.decode("latin-1", errors="replace").strip()
    try:
        text = str(value).strip()
        if _MOJIBAKE_RE.search(text):
            text = _fix_mojibake(text)
        return text
    except Exception:
        return ""


def _safe_scalar(value) -> str:
    """Like _safe_str but collapses internal newlines to a single space.
    Use for fields that must be single-line in CSV output: Subject,
    FromName, FromAddress etc. Do NOT use for transport_headers which
    depends on newlines for header field boundary detection.
    """
    text = _safe_str(value)
    if '\n' in text or '\r' in text:
        text = re.sub(r'\r\n|\r|\n', ' ', text).strip()
    return text


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


_RTF_HEADER_RE  = re.compile(r'^\s*\{\\rtf', re.IGNORECASE)
_HTML_HEAD_RE      = re.compile(r'<head[\s>].*?</head>',                  re.IGNORECASE | re.DOTALL)
_HTML_STYLE_RE     = re.compile(r'<style[\s>].*?</style>',                re.IGNORECASE | re.DOTALL)
_HTML_SCRIPT_RE    = re.compile(r'<script[\s>].*?</script>',              re.IGNORECASE | re.DOTALL)
_HTML_MSO_XML_RE   = re.compile(r'<!--\[if[^\]]*\]>.*?<!\[endif\]-->', re.IGNORECASE | re.DOTALL)
_HTML_XML_RE       = re.compile(r'<xml[\s>].*?</xml>',                    re.IGNORECASE | re.DOTALL)
_HTML_TAG_RE       = re.compile(r'<[^>]+')
_RTF_CONTROL_RE = re.compile(r'\{[^{}]*\}|\\[a-z]+\d*\s?|[{}]')


def _strip_rtf(text: str) -> str:
    # Best-effort plain text extraction from an RTF string.
    plain = _RTF_CONTROL_RE.sub(' ', text)
    plain = re.sub(r'[ \t]+', ' ', plain)
    plain = re.sub(r'\n{3,}', '\n\n', plain)
    return plain.strip()


def _strip_html(text: str) -> str:
    # Strip HTML to plain text, removing Word-generated head/style blocks.
    # Also removes MSO conditional comment blocks (<!--[if gte mso 9]>...)
    # and <xml> blocks containing w:LsdException schema definitions —
    # these survive <head> stripping and cause CSV row splits via the
    # double-quote escaping of their XML attributes.
    text = _HTML_MSO_XML_RE.sub('', text)
    text = _HTML_XML_RE.sub('', text)
    text = _HTML_HEAD_RE.sub('', text)
    text = _HTML_STYLE_RE.sub('', text)
    text = _HTML_SCRIPT_RE.sub('', text)
    text = _HTML_TAG_RE.sub(' ', text)
    text = (text
            .replace('&nbsp;', ' ')
            .replace('&amp;',  '&')
            .replace('&lt;',   '<')
            .replace('&gt;',   '>')
            .replace('&quot;', '"')
            .replace('&#39;',  "'"))
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def _clean_body(text: str) -> str:
    # Sanitise email body for safe CSV output.
    # Strips RTF/HTML to plain text and collapses all newlines to spaces
    # so TextBody is always a single line — Excel splits rows on bare \n
    # inside quoted fields regardless of the RFC 4180 spec.
    if not text:
        return ''
    text = text.replace('\x00', '').replace('\ufeff', '')
    text = re.sub(r'[\x01-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    if _RTF_HEADER_RE.match(text):
        text = _strip_rtf(text)
    elif '<' in text and '>' in text and re.search(r'<[a-zA-Z]', text):
        text = _strip_html(text)
    # Collapse all newline variants to a single space
    text = re.sub(r'\r\n|\r|\n', ' ', text)
    text = re.sub(r' {2,}', ' ', text)
    return text.strip()


# Matches base64-encoded data: URIs in HTML src/href attributes.
# These inline image blobs can be 10,000-30,000+ characters and
# cause field-length explosions and row splits in CSV output.
_DATA_URI_RE = re.compile(
    r'(src|href)=(["\'])data:[^;]+;base64,[A-Za-z0-9+/=\s]+\2',
    re.IGNORECASE
)


def _clean_html_body(text: str) -> str:
    """Sanitise an HTML email body for Salesforce HtmlBody.

    Preserves all HTML tags. Strips base64 data: URIs (inline images)
    which can be tens of thousands of characters and break CSV row
    boundaries. Collapses newlines to spaces for single-line output.
    """
    if not text:
        return ''
    # Remove null bytes, BOM, and non-printable control characters
    text = text.replace('\x00', '').replace('\ufeff', '')
    text = re.sub(r'[\x01-\x08\x0b\x0c\x0e-\x1f\x7f]', '', text)
    # Strip MSO conditional comment blocks and <xml> schema blocks —
    # these contain w:LsdException attributes with ""double-quoted"" values
    # that break CSV parsers even inside quoted fields.
    text = _HTML_MSO_XML_RE.sub('', text)
    text = _HTML_XML_RE.sub('', text)
    # Strip base64 data: URIs — replace with placeholder so HTML stays valid
    text = _DATA_URI_RE.sub(r'\1="[embedded-image]"', text)
    # Collapse all newline variants to a single space
    text = re.sub(r'\r\n|\r|\n', ' ', text)
    text = re.sub(r'[ \t]{2,}', ' ', text)
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


# Parses a named header field (To, Cc, Bcc) from raw transport headers
# Handles folded headers (continuation lines starting with whitespace)
_HEADER_FIELD_RE = re.compile(
    r'^({field}):\s*(.+?)(?=\n\S|\Z)', re.IGNORECASE | re.MULTILINE | re.DOTALL
)


def _parse_header_addresses(headers: str, field: str) -> str:
    """Extract a semicolon-delimited address list from a named header field."""
    pattern = re.compile(
        rf'^{re.escape(field)}:\s*(.+?)(?=\n[^\t ]|\Z)',
        re.IGNORECASE | re.MULTILINE | re.DOTALL,
    )
    m = pattern.search(headers)
    if not m:
        return ""
    # Unfold continuation lines, split on commas, extract bare addresses
    raw = re.sub(r'\r?\n[\t ]', ' ', m.group(1)).strip()
    addrs = []
    for part in raw.split(','):
        part = part.strip()
        # "Display Name <addr@example.com>" -> addr@example.com
        angle = re.search(r'<([^>]+)>', part)
        if angle:
            addrs.append(angle.group(1).strip().lower())
        elif '@' in part:
            addrs.append(part.lower())
    return ';'.join(addrs)


# Detects CSS/stylesheet content leaking into subject or body fields.
# Occurs with malformed HTML emails where Outlook stored style definitions
# in MAPI subject/body properties instead of proper email content.
_CSS_CONTENT_RE = re.compile(
    r'mso-|'
    r'\.Mso[A-Z]|li\.mso|span\.|'
    r'@font-face|font-family:|text-decoration:|'
    r'color:#[0-9a-fA-F]{3,6}|margin:|padding:|font-size:',
    re.IGNORECASE
)


def _parse_header_recipients(headers: str, field: str) -> list:
    """Return list of (display_name, email_address) tuples from a header field.
    Handles: 'Display Name <addr>', '"Name" <addr>', bare 'addr@example.com'.
    """
    pattern = re.compile(
        rf'^{re.escape(field)}:\s*(.+?)(?=\n[^\t ]|\Z)',
        re.IGNORECASE | re.MULTILINE | re.DOTALL,
    )
    m = pattern.search(headers)
    if not m:
        return []
    raw = re.sub(r'\r?\n[\t ]', ' ', m.group(1)).strip()
    results = []
    for part in raw.split(','):
        part = part.strip()
        if not part:
            continue
        angle = re.search(r'^(.*?)<([^>]+)>', part)
        if angle:
            name = angle.group(1).strip().strip('"')
            addr = angle.group(2).strip().lower()
            results.append((name, addr))
        elif '@' in part:
            results.append(('', part.strip().lower()))
    return results


# Filenames matching these patterns are junk — Word temp files, OLE internals,
# tracking pixels, and Windows system files that end up as MAPI attachments.
_JUNK_FILENAME_RE = re.compile(
    r'^~'               # Word temp/lock files (~WRD0000.jpg, ~$document.docx)
    r'|^Thumbs\.db$'   # Windows thumbnail cache
    r'|^desktop\.ini$' # Windows folder settings
    r'|\.tmp$'         # Generic temp files
    r'|^image\d+\.wmz$'  # Word internal metafile images
    r'|^oledata\.mso$' # OLE compound document stub
    , re.IGNORECASE
)
# Small images under this size are treated as tracking pixels / decorations
_TRACKING_PIXEL_EXTENSIONS = {'.gif', '.png', '.jpg', '.jpeg', '.bmp', '.wmz'}
_TRACKING_PIXEL_MAX_BYTES  = 1024


def _is_junk_attachment(filename: str, size_bytes: int) -> bool:
    """Return True if the attachment should be excluded from the CSV output.
    Filters Word temp files, OLE stubs, tracking pixels, and Windows
    system files that Outlook stores as MAPI attachments internally.
    """
    if not filename:
        return False
    if _JUNK_FILENAME_RE.search(filename):
        return True
    ext = ('.' + filename.rsplit('.', 1)[-1].lower()) if '.' in filename else ''
    if ext in _TRACKING_PIXEL_EXTENSIONS and size_bytes < _TRACKING_PIXEL_MAX_BYTES:
        return True
    return False


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
            folder_name = _safe_str(folder.get_name()) or "Root"
        except Exception:
            folder_name = "Unknown"

        current_path = f"{folder_path}/{folder_name}".lstrip("/")

        # Process messages in this folder
        num_messages = folder.get_number_of_sub_messages()
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
        for j in range(folder.get_number_of_sub_folders()):
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
        def _get(fn):
            """Call a pypff getter; decode via _safe_str (preserves newlines)."""
            try:
                return _safe_str(fn())
            except Exception:
                return ""

        def _get_scalar(fn):
            """Call a pypff getter; collapse newlines — for single-line fields."""
            try:
                return _safe_scalar(fn())
            except Exception:
                return ""

        def _get_dt(fn):
            try:
                return _safe_dt(fn())
            except Exception:
                return ""

        subject      = _get_scalar(message.get_subject)
        sender       = _get_scalar(message.get_sender_name)
        sender_email = _parse_sender_email(message)

        # Body getters return bytes — _safe_str decodes and repairs encoding.
        # body_plain: strip RTF/HTML markup to produce clean plain text.
        # body_html:  preserve full HTML tags — only fix encoding and null bytes.
        body_plain = _clean_body(_get(message.get_plain_text_body))
        try:
            body_html = _clean_html_body(_get(message.get_html_body))
        except OSError:
            body_html = ""

        # client_submit_time = when sender sent; delivery_time = when received.
        sent_dt = (
            _get_dt(message.get_client_submit_time)
            or _get_dt(message.get_delivery_time)
        )

        # Pre-parse To/Cc/Bcc from transport_headers as a reliable fallback.
        # get_email_address() on recipient sub-items often returns empty for
        # Exchange/X.400 addresses — the headers always have SMTP addresses.
        try:
            _hdrs = _safe_str(message.get_transport_headers())
        except Exception:
            _hdrs = ""
        _to_hdr  = _parse_header_addresses(_hdrs, "To")  if _hdrs else ""
        _cc_hdr  = _parse_header_addresses(_hdrs, "Cc")  if _hdrs else ""
        _bcc_hdr = _parse_header_addresses(_hdrs, "Bcc") if _hdrs else ""
        try:
            num_attach = message.get_number_of_attachments() or 0
        except Exception:
            num_attach = 0
        has_attach = num_attach > 0

        # Skip non-email items (calendar, contacts, tasks, notes).
        # Real emails always have at least one of: subject, sender, or body.
        # Items missing all three are MAPI non-mail entries that landed in
        # a message slot — processing them produces blank rows.
        if not any([subject, sender, sender_email, body_plain, body_html, sent_dt]):
            log.debug("Skipping non-email item in '%s' (no subject/sender/body/date)",
                      folder_path)
            return

        # Skip items where the subject contains CSS/stylesheet content.
        # These are malformed HTML emails where Outlook stored Word stylesheet
        # definitions (li.MsoNormal, mso-style-priority etc.) in the subject
        # MAPI property — they produce rows of CSS noise, not real emails.
        if subject and _CSS_CONTENT_RE.search(subject):
            log.debug("Skipping CSS/stylesheet item in '%s' (subject: %s)",
                      folder_path, subject[:60])
            return

        self.emails.append({
            "Id":              email_id,
            "Subject":         subject,
            "SenderName":      sender,
            "SenderEmail":     sender_email,
            "SentDate":        sent_dt,
            "BodyPlain":       body_plain,
            "BodyHtml":        body_html,
            # Seeded from transport_headers; overwritten in main() if
            # recipient sub-items yield better SMTP addresses.
            "ToAddress":       _to_hdr,
            "CcAddress":       _cc_hdr,
            "BccAddress":      _bcc_hdr,
            "IsClientManaged": True,
            "FolderPath":      folder_path,
        })

        # ---- Recipients -------------------------------------------------
        self._extract_recipients(message, email_id, _hdrs)

        # ---- Attachments ------------------------------------------------
        if has_attach:
            self._extract_attachments(message, email_id)

    # ------------------------------------------------------------------
    def _extract_recipients(self, message, email_id: str, transport_headers: str = ''):
        """Parse To / CC / BCC recipients.

        Primary source: message.get_recipients() container sub-items.
        Fallback: parse To:/Cc:/Bcc: directly from transport_headers,
        which always contains SMTP addresses even when the MAPI recipient
        object returns empty email addresses (Exchange/X.400 internal).
        """
        def _rget(fn):
            try:
                return _safe_scalar(fn())
            except Exception:
                return ""

        added = 0

        # ---- Primary: MAPI recipient sub-items --------------------------
        try:
            recipients = message.get_recipients()
            if recipients is not None:
                num_recip = recipients.get_number_of_sub_items()
                type_map = {0: "To", 1: "CC", 2: "BCC"}

                for i in range(num_recip):
                    try:
                        recip = recipients.get_sub_item(i)
                        try:
                            recip_type_raw = int(recip.get_recipient_type() or 0)
                        except Exception:
                            recip_type_raw = 0
                        recip_type = type_map.get(recip_type_raw, "To")
                        display  = _rget(recip.get_display_name)
                        address  = _rget(recip.get_email_address)
                        if display or address:
                            self.recipients.append({
                                "Id":            str(uuid.uuid4()),
                                "EmailId":       email_id,
                                "RecipientType": recip_type,
                                "DisplayName":   display,
                                "EmailAddress":  address,
                            })
                            added += 1
                    except Exception as exc:
                        log.debug("Recipient sub-item %d error: %s", i, exc)
        except Exception as exc:
            log.debug("get_recipients() failed for email %s: %s", email_id, exc)

        # ---- Fallback: parse transport_headers --------------------------
        # Used when MAPI recipients returned nothing or had empty addresses.
        # Uses the headers already parsed in _process_message — calling
        # get_transport_headers() twice on the same object returns empty
        # on some pypff builds (internal buffer cursor not reset).
        if added == 0:
            hdrs = transport_headers

            if hdrs:
                type_to_relation = {
                    "To":  "To",
                    "Cc":  "CC",
                    "Bcc": "BCC",
                }
                for field, recip_type in type_to_relation.items():
                    for name, addr in _parse_header_recipients(hdrs, field):
                        if addr:
                            self.recipients.append({
                                "Id":            str(uuid.uuid4()),
                                "EmailId":       email_id,
                                "RecipientType": recip_type,
                                "DisplayName":   name,
                                "EmailAddress":  addr,
                            })
                            added += 1

        if added == 0:
            log.debug("No recipients found for email %s", email_id)

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
        try:
            num_attach = message.get_number_of_attachments() or 0
        except Exception:
            num_attach = 0

        for i in range(num_attach):
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

                # ---- Junk filter ----------------------------------------
                # Skip Word temp files, OLE stubs, and tracking pixels.
                # Size is not yet known here so pass 0 — name-based rules
                # still apply. Size-based check runs after read_buffer.
                if _is_junk_attachment(raw_name, 0):
                    log.debug("Skipping junk attachment '%s' on email %s",
                              filename, email_id)
                    continue

                # ---- Attachment method -----------------------------------
                try:
                    attach_method = int(attach.get_attachment_method() or 0)
                except AttributeError:
                    try:
                        attach_method = int(attach.attachment_method or 0)
                    except Exception:
                        attach_method = 0
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
                            # Size-based junk check — small images are
                            # tracking pixels or inline decorations
                            if _is_junk_attachment(filename, size_bytes):
                                log.debug(
                                    "Skipping tracking pixel '%s' (%d bytes)",
                                    filename, size_bytes
                                )
                                continue
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

    columns must be the SOURCE key names in the desired output order.
    Rows are filtered to only those keys before renaming, so no orphan
    internal fields can bleed into the output.
    """
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=columns)

    # Select only the declared source columns in declared order
    src_cols = [c for c in columns if c in df.columns]
    df = df[src_cols]

    # Rename source keys to Salesforce API names
    if rename:
        df = df.rename(columns={k: v for k, v in rename.items() if k in src_cols})

    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].map({True: "TRUE", False: "FALSE"})
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL,
              doublequote=True, lineterminator="\r\n")
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
        for i in range(folder.get_number_of_sub_messages()):
            return folder.get_sub_message(i)
        for j in range(folder.get_number_of_sub_folders()):
            msg = find_first(folder.get_sub_folder(j), depth+1)
            if msg:
                return msg

    msg = find_first(root)
    if not msg:
        print("No messages found in PST.")
        pst.close()
        return

    print(f"  number_of_sub_messages (folder): {root.get_number_of_sub_messages()}")
    try:
        print(f"  get_number_of_attachments(): {msg.get_number_of_attachments()}")
    except Exception as e:
        print(f"  get_number_of_attachments() ERROR: {e}")
    try:
        recip_container = msg.get_recipients()
        n = recip_container.get_number_of_sub_items() if recip_container else 0
        print(f"  get_recipients().get_number_of_sub_items(): {n}")
    except Exception as e:
        print(f"  get_recipients() ERROR: {e}")

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

    try:
        _rc = msg.get_recipients()
        _nr = _rc.get_number_of_sub_items() if _rc else 0
    except Exception:
        _rc, _nr = None, 0
    if _nr > 0:
        print("\n--- First recipient ---")
        r = _rc.get_sub_item(0)
        for g in ["get_display_name", "get_email_address", "get_recipient_type", "get_name"]:
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
        # Only overwrite header-parsed addresses if recipient sub-items
        # produced non-empty SMTP addresses (they're more structured).
        if addrs.get("ToAddress"):
            email["ToAddress"]  = addrs["ToAddress"]
        if addrs.get("CcAddress"):
            email["CcAddress"]  = addrs["CcAddress"]
        if addrs.get("BccAddress"):
            email["BccAddress"] = addrs["BccAddress"]
        # IsClientManaged already set to True in _process_message

    # ---- 1. emails.csv  →  EmailMessage (Insert) ------------------------
    # BodyHtml included/excluded BEFORE list() so columns and rename stay in sync
    email_col_map = {
        "Id":              "ExternalId__c",
        "Subject":         "Subject",
        "SenderName":      "FromName",
        "SenderEmail":     "FromAddress",
        "SentDate":        "MessageDate",
        "BodyPlain":       "TextBody",
        "ToAddress":       "ToAddress",
        "CcAddress":       "CcAddress",
        "BccAddress":      "BccAddress",
        "IsClientManaged": "IsClientManaged",
        "FolderPath":      "Description",
    }
    if not args.no_body_html:
        email_col_map["BodyHtml"] = "HtmlBody"

    write_csv(extractor.emails, list(email_col_map.keys()),
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

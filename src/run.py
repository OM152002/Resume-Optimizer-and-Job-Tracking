import os
import json
import pathlib
import subprocess
import uuid
import re
import traceback
from tenacity import RetryError
from datetime import datetime, timezone

from .latex_validate import looks_like_latex_resume
from .llm_gemini import generate_apply_pack
from .notion_client import (
    get_database_schema,
    build_property_index,
    get_page,
    fetch_by_status,
    update_page_safe,
)

ART_DIR = pathlib.Path("artifacts")
ART_DIR.mkdir(exist_ok=True)


def chunk_rich_text(s: str, chunk: int = 1900):
    s = s or ""
    return [{"text": {"content": s[i:i+chunk]}} for i in range(0, min(len(s), 6000), chunk)]


def sh(cmd: list[str], cwd: str | None = None) -> str:
    r = subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=cwd)
    return (r.stdout or "").strip()

def explain_exception(e: Exception) -> str:
    # Tenacity wraps the real exception inside RetryError
    if isinstance(e, RetryError):
        last = e.last_attempt.exception()
        if last:
            return f"{type(last).__name__}: {last}"
        return "RetryError: last_attempt had no exception"
    return f"{type(e).__name__}: {e}"


def safe_text(prop) -> str:
    try:
        if not prop:
            return ""
        t = prop.get("type")
        if t == "title":
            parts = prop.get("title") or []
            return "".join([p.get("plain_text", "") for p in parts])
        if t == "rich_text":
            parts = prop.get("rich_text") or []
            return "".join([p.get("plain_text", "") for p in parts])
        parts = prop.get("rich_text") or prop.get("title") or []
        return "".join([p.get("plain_text", "") for p in parts])
    except Exception:
        return ""


def get_url(prop) -> str:
    try:
        return (prop or {}).get("url") or ""
    except Exception:
        return ""


def clean_path_segment(s: str) -> str:
    s = (s or "").strip().replace("/", "-").replace("\\", "-")
    s = "_".join(s.split())
    return s[:80] if s else "Unknown"


def find_prop(props: dict, candidates: list[str]):
    for c in candidates:
        if c in props:
            return props[c]

    def norm(x: str) -> str:
        return " ".join((x or "").strip().lower().split())

    props_norm = {norm(k): k for k in props.keys()}
    for c in candidates:
        nk = norm(c)
        if nk in props_norm:
            return props[props_norm[nk]]

    for c in candidates:
        for k in props.keys():
            if k.strip() == c.strip():
                return props[k]
    return None


def normalize_unicode(s: str) -> str:
    if not s:
        return s
    return (
        s.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2018", "'")
        .replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2022", "-")
        .replace("\u00d7", "x")
        .replace("\u00a0", " ")
    )


def escape_tex_specials(latex: str) -> str:
    """
    Robust escaping for special characters that frequently break AI-generated LaTeX.
    Escapes only when NOT already escaped.
    """
    # Escape & % $ # _
    replacements = {
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "^": r"\textasciicircum{}",
        "~": r"\textasciitilde{}",
    }
    out = latex
    for ch, rep in replacements.items():
        # replace any occurrence not preceded by backslash
        out = re.sub(rf"(?<!\\){re.escape(ch)}", rep, out)
    return out


def sanitize_latex(tex: str) -> str:
    if tex is None:
        return ""
    tex = tex.replace("\ufeff", "").replace("\x00", "")
    tex = tex.strip()
    # Replace common math commands that break text mode
    # Fix double-escaped newlines and commands
    # We use a regex to handle \\ -> \ and \n -> newline safely.
    # We protect \n if it is followed by a letter (likely a command like \newcommand).
    def unescape(m):
        if m.group(1): return "\\"  # Matches \\
        return "\n"                 # Matches \n not followed by letter
    
    tex = re.sub(r"(\\\\)|(\\n(?![a-zA-Z]))", unescape, tex)
    
    # Fix common escaped characters that might have been double-escaped
    tex = tex.replace(r"\\%", r"\%").replace(r"\\&", r"\&").replace(r"\\$", r"\$").replace(r"\\#", r"\#").replace(r"\\_", r"\_")

    # remove markdown code fences if present
    tex = re.sub(r"^\s*```[a-zA-Z0-9_-]*\s*\n", "", tex)
    tex = re.sub(r"\n\s*```\s*$", "", tex)

    # trim to document boundaries if present
    start = tex.find(r"\documentclass")
    if start != -1:
        tex = tex[start:]
    end = tex.rfind(r"\end{document}")
    if end != -1:
        tex = tex[: end + len(r"\end{document}")]

    # if model accidentally drops leading backslash on the first line
    lines = tex.splitlines()
    if lines:
        l0 = lines[0].lstrip()
        if l0.startswith("documentclass"):
            lines[0] = lines[0].replace("documentclass", r"\documentclass", 1)
        if l0.startswith("usepackage"):
            lines[0] = lines[0].replace("usepackage", r"\usepackage", 1)
    tex = "\n".join(lines)

    tex = normalize_unicode(tex)
    # tex = escape_tex_specials(tex) # DO NOT ESCAPE SPECIALS IN FULL LATEX DOC

    # hard fail early with a clearer error
    if not tex.lstrip().startswith(r"\documentclass"):
        head = "\n".join(tex.splitlines()[:5])
        raise RuntimeError(
            "Generated LaTeX does not start with \\documentclass. First lines:\n"
            + head
        )
    if r"\end{document}" not in tex:
        raise RuntimeError("Generated LaTeX missing \\end{document}.")

    return tex


def merge_with_master_preamble(master: str, tex: str) -> str:
    start = master.find(r"\begin{document}")
    if start == -1:
        raise RuntimeError("Master LaTeX missing \\begin{document}.")
    preamble = master[: start + len(r"\begin{document}")]

    b_start = tex.find(r"\begin{document}")
    b_end = tex.rfind(r"\end{document}")
    if b_start != -1 and b_end != -1 and b_end > b_start:
        body = tex[b_start + len(r"\begin{document}") : b_end]
    else:
        body = tex

    body = body.strip()
    return preamble + "\n" + body + "\n" + r"\end{document}"

def count_itemize_items(latex: str) -> int:
    # counts \coloredbullet occurrences
    return len(re.findall(r"\\coloredbullet", latex))

def require_same_section_markers(master: str, tailored: str) -> None:
    required = [
        r"\section{Education}",
        r"\section{Skills}",
        r"\section{Experience}",
        r"\section{Notable Projects}",
    ]
    for m in required:
        if m not in tailored:
            raise RuntimeError(f"mutation_violation: missing section marker {m}")

    # ensure all required markers exist in master too (sanity)
    for m in required:
        if m not in master:
            raise RuntimeError(f"pipeline_error: master missing section marker {m}")

def require_no_new_companies(master: str, tailored: str) -> None:
    # simplest guard: tailored cannot contain new \datedexperience entries
    # compare all \datedexperience{...} lines
    master_companies = set(re.findall(r"\\datedexperience\{([^}]+)\}", master))
    tailored_companies = set(re.findall(r"\\datedexperience\{([^}]+)\}", tailored))
    
    new_companies = sorted([c for c in tailored_companies if c not in master_companies])
    if new_companies:
        raise RuntimeError("mutation_violation: introduced new companies: " + ", ".join(new_companies[:10]))

def require_bullet_count_stable(
    master: str, tailored: str, max_drop: int = 10, max_add: int = 10
) -> None:
    m = count_itemize_items(master)
    t = count_itemize_items(tailored)
    if t < m - max_drop or t > m + max_add:
        raise RuntimeError(f"mutation_violation: bullet_count master={m} tailored={t}")


def compile_pdf(tex_path: pathlib.Path) -> pathlib.Path:
    out_dir = tex_path.parent
    try:
        # Copy and patch .cls file to out_dir if it exists in templates
        cls_src = pathlib.Path(__file__).parent / "templates" / "om_patel.cls"
        if cls_src.exists():
            cls_content = cls_src.read_text(encoding="utf-8")
            # Patch the class name to match the filename
            cls_content = cls_content.replace(r"\ProvidesClass{muratcan_cv}", r"\ProvidesClass{om_patel}")
            
            cls_dest = out_dir / "om_patel.cls"
            cls_dest.write_text(cls_content, encoding="utf-8")
            print(f"Copied and patched {cls_src} to {cls_dest}")
        else:
            print(f"WARNING: Class file not found at {cls_src}")

        # Debug: List files in out_dir to confirm existence
        print(f"Files in {out_dir}:")
        subprocess.run(["ls", "-la", str(out_dir)], check=False)

        subprocess.run(
            ["tectonic", tex_path.name, "--outdir", "."],
            cwd=str(out_dir),
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        combined = ((e.stdout or "") + "\n" + (e.stderr or "")).strip()
        tail = combined[-1800:] if len(combined) > 1800 else combined
        context = ""
        # Prefer "error:" lines for context, falling back to any file:line match
        match = re.search(rf"error: {re.escape(tex_path.name)}:(\d+)", combined)
        if not match:
            match = re.search(rf"{re.escape(tex_path.name)}:(\d+)", combined)
        if match and tex_path.exists():
            try:
                line_no = int(match.group(1))
                lines = tex_path.read_text(encoding="utf-8").splitlines()
                start = max(line_no - 3, 0)
                end = min(line_no + 2, len(lines))
                snippet = []
                for i in range(start, end):
                    snippet.append(f"{i + 1}: {lines[i]}")
                context = "\n\nContext:\n" + "\n".join(snippet)
            except Exception:
                context = ""
        raise RuntimeError("tectonic_failed:\n" + tail + context) from e

    pdf_path = out_dir / tex_path.with_suffix(".pdf").name
    if not pdf_path.exists():
        raise RuntimeError("tectonic_failed: PDF not produced")
    return pdf_path


def main():
    limit = int(os.getenv("LIMIT") or "5")

    run_id = uuid.uuid4().hex[:10]
    model_name = os.getenv("MODEL_NAME", os.getenv("GEMINI_MODEL", "gemini"))
    prompt_version = os.getenv("PROMPT_VERSION", "v1")

    # Load templates
    templates_dir = pathlib.Path(__file__).parent / "templates"
    master_latex_path = templates_dir / "Om_Patel_Resume.tex"
    if not master_latex_path.exists():
        raise RuntimeError(f"Master LaTeX not found at {master_latex_path}")
    master_latex = master_latex_path.read_text(encoding="utf-8")

    remote = os.environ["RCLONE_REMOTE"]  # e.g. gdrive
    drive_root = os.environ.get("DRIVE_ROOT", "JobApps")

    schema = get_database_schema()
    idx = build_property_index(schema)

    page_id_env = (os.getenv("PAGE_ID") or "").strip()

    if page_id_env:
        # fetch single page
        item = get_page(page_id_env)
        items = [item]
    else:
        items = fetch_by_status("Not Applied", limit=limit, idx=idx)

    run_log = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "processed": 0,
        "ok": 0,
        "errors": 0,
        "details": [],
    }

    for page in items:
        page_id = page["id"]
        props = page.get("properties", {}) or {}

        company = safe_text(find_prop(props, ["Company"]) or {})
        role = safe_text(find_prop(props, ["Role"]) or {})

        url = (
            get_url(find_prop(props, ["Job URL"]) or {})
            or get_url(find_prop(props, ["Job Link"]) or {})
            or get_url(find_prop(props, ["URL"]) or {})
        )

        jd = safe_text(find_prop(props, ["Job Description", "JD", "Description"]) or {})

        if not jd.strip():
            info = update_page_safe(
                page_id,
                {
                    "Status": "Error",
                    "Errors": "Job Description is empty or unreadable by API",
                    "Run ID": run_id,
                    "Model": model_name,
                    "Prompt version": prompt_version,
                },
                idx,
            )
            run_log["errors"] += 1
            run_log["processed"] += 1
            run_log["details"].append(
                {"page": page_id, "status": "error", "reason": "empty_jd", "notion": info}
            )
            continue

        try:
            pack = generate_apply_pack(
                master_latex=master_latex,
                jd=jd,
                company=company,
                role=role,
                url=url,
            )
            print("LLM_KEYS", sorted(list(pack.keys())))
            required = ["tailored_latex", "fit_score", "keyword_coverage", "outreach"]
            missing = [k for k in required if k not in pack]
            if missing:
                raise RuntimeError(
                    f"LLM JSON missing fields: {missing}. Keys={list(pack.keys())}"
                )

            tailored_raw = (
                pack.get("tailored_latex")
                or pack.get("document")   # backward compat if you ever switch schemas
                or pack.get("latex")
                or ""
            )
            if not tailored_raw:
                raise RuntimeError(
                    f"LLM output missing LaTeX field. Keys={list(pack.keys())}"
                )
            tailored_latex = sanitize_latex(tailored_raw)
            tailored_latex = merge_with_master_preamble(master_latex, tailored_latex)

            ok, reason = looks_like_latex_resume(tailored_latex)
            # Mutation guards: fail fast if AI changed structure
            require_same_section_markers(master_latex, tailored_latex)
            try:
                require_bullet_count_stable(
                    master_latex, tailored_latex, max_drop=10, max_add=10
                )
            except RuntimeError:
                pack = generate_apply_pack(
                    master_latex=master_latex,
                    jd=jd,
                    company=company,
                    role=role,
                    url=url,
                    force_same_bullets=True,
                )
                print("LLM_KEYS", sorted(list(pack.keys())))
                required = ["tailored_latex", "fit_score", "keyword_coverage", "outreach"]
                missing = [k for k in required if k not in pack]
                if missing:
                    raise RuntimeError(
                        f"LLM JSON missing fields: {missing}. Keys={list(pack.keys())}"
                    )

                tailored_raw = (
                    pack.get("tailored_latex")
                    or pack.get("document")   # backward compat if you ever switch schemas
                    or pack.get("latex")
                    or ""
                )
                if not tailored_raw:
                    raise RuntimeError(
                        f"LLM output missing LaTeX field. Keys={list(pack.keys())}"
                    )
                tailored_latex = sanitize_latex(tailored_raw)

                ok, reason = looks_like_latex_resume(tailored_latex)
                require_same_section_markers(master_latex, tailored_latex)
                require_bullet_count_stable(
                    master_latex, tailored_latex, max_drop=10, max_add=10
                )

            fit_score = pack.get("fit_score", 0)
            kw_cov = pack.get("keyword_coverage", 0)

            outreach = pack.get("outreach", {})
            outreach_block = "\n\n".join(
                [
                    f"LinkedIn connect note:\n{outreach.get('linkedin_connect_note','')}",
                    f"LinkedIn message:\n{outreach.get('linkedin_message','')}",
                    f"Recruiter email:\n{outreach.get('recruiter_email','')}",
                    f"Follow-up (7d):\n{outreach.get('followup_7d','')}",
                    f"Follow-up (14d):\n{outreach.get('followup_14d','')}",
                ]
            )

            if not ok:
                info = update_page_safe(
                    page_id,
                    {
                        "Status": "Error",
                        "Errors": f"LaTeX invalid: {reason}",
                        "Run ID": run_id,
                        "Model": model_name,
                        "Prompt version": prompt_version,
                    },
                    idx,
                )
                run_log["errors"] += 1
                run_log["processed"] += 1
                run_log["details"].append(
                    {"page": page_id, "status": "error", "reason": reason, "notion": info}
                )
                continue

            out_dir = ART_DIR / f"{clean_path_segment(company)}_{clean_path_segment(role)}"
            out_dir.mkdir(parents=True, exist_ok=True)

            tex_path = out_dir / "Om_Patel_Resume.tex"
            tex_path.write_text(tailored_latex, encoding="utf-8")

            pdf_path = compile_pdf(tex_path)

            # Drive destination: JobApps/<Company>/<Role>
            dest_dir = f"{drive_root}/{clean_path_segment(company)}/{clean_path_segment(role)}"

            sh(["rclone", "mkdir", f"{remote}:{dest_dir}"])
            sh(["rclone", "copyto", str(pdf_path), f"{remote}:{dest_dir}/{pdf_path.name}"])
            sh(["rclone", "copyto", str(tex_path), f"{remote}:{dest_dir}/{tex_path.name}"])

            pdf_link = sh(["rclone", "link", f"{remote}:{dest_dir}/{pdf_path.name}"])
            tex_link = sh(["rclone", "link", f"{remote}:{dest_dir}/{tex_path.name}"])

            info = update_page_safe(
                page_id,
                {
                    "Status": "Ready",
                    "Errors": "",
                    "Fit score": float(fit_score),
                    "Keyword Coverage": float(kw_cov),
                    "Follow up message": chunk_rich_text(outreach_block),
                    "Run ID": run_id,
                    "Model": model_name,
                    "Prompt version": prompt_version,
                    "Resume PDF": pdf_link,
                    "Resume Latex": tex_link,
                },
                idx,
            )

            run_log["ok"] += 1
            run_log["processed"] += 1
            run_log["details"].append(
                {
                    "page": page_id,
                    "status": "ok",
                    "company": company,
                    "role": role,
                    "url": url,
                    "tex": str(tex_path),
                    "pdf": str(pdf_path),
                    "drive_pdf": pdf_link,
                    "drive_tex": tex_link,
                    "notion": info,
                }
            )

        except Exception as e:
            trace = traceback.format_exc()

            # print to Actions logs (full fidelity)
            print(trace)

            # Notion rich_text blocks must be <=2000 chars each; keep it short
            short = trace[-1800:]  # tail is usually the most relevant

            info = update_page_safe(
                page_id,
                {
                    "Status": "Error",
                    "Errors": short,
                    "Run ID": run_id,
                    "Model": model_name,
                    "Prompt version": prompt_version,
                },
                idx,
            )

            run_log["errors"] += 1
            run_log["processed"] += 1
            run_log["details"].append(
                {
                    "page": page_id,
                    "status": "error",
                    "reason": f"{type(e).__name__}: {e}",
                    "trace": trace,
                    "notion": info,
                }
            )


    (ART_DIR / "run_log.json").write_text(json.dumps(run_log, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

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

MASTER_LATEX = r"""
\documentclass[10.5pt]{article}
\usepackage[margin=0.4in, bottom=0.4in, top=0.5in]{geometry}
\usepackage{hyperref}
\usepackage{enumitem}
\usepackage{titlesec}
\usepackage{parskip}
\usepackage{comment}

\pagenumbering{gobble}

\titleformat{\section}{\large\bfseries}{}{0em}{}
\titleformat{\subsection}{\bfseries}{}{0em}{}

\setlength{\parindent}{0pt}

\title{\vspace{-1.6cm}\textbf {Poojan Vanani}}
\date{}

\begin{document}

\maketitle

\vspace{-2cm}

\hrule
\begin{center}
\small
    +1 (224)400-2468 
    \quad \href{mailto:poojan.vanani1900@gmail.com}{poojan.vanani1900@gmail.com}  
    \quad \href{https://www.linkedin.com/in/poojan-vanani}{www.linkedin.com/in/poojan-vanani} 
    \quad \href{https://poojanvanani.tech}{https://poojanvanani.tech}
\end{center}

\vspace{-10pt}
\section*{SUMMARY}
\vspace{-5pt}
\hrule
Mechanical Design \& Robotics Engineer with 3.5+ years of experience delivering precision electromechanical systems for automation, biofabrication, and research. Skilled in CAD, GD\&T, high-volume manufacturing, and cross-functional product launches with proven cost, quality, and time-to-market improvements.

\vspace{-10pt}
\section*{EDUCATION}
\vspace{-5pt}
\hrule

\textbf{M.S. in Robotics and Autonomous Systems (Mechanical And Aerospace Engineering) } \hfill 8/2023 - 5/2025 \\
Arizona State University, Tempe, AZ \hfill GPA: 3.47/4.00

\vspace{-4pt}

\textbf{Bachelor of Technology in Mechatronics Engineering} \hfill 7/2018 - 5/2022 \\
Ganpat University, Mehsana, Gujarat, India \hfill GPA: 8.07/10 

\vspace{-10pt}
\section*{TECHNICAL SKILLS}
\vspace{-5pt}
\hrule

\begin{itemize}[left=5pt,itemsep=-4pt]
\item \textbf{Mechanical Design:} SolidWorks (CSWA), Onshape, CATIA (familiar), AutoCAD, 3D modeling, GD\&T, FEA, DFM/DFA, Tolerance Stack-up, Injection Molding, Die Casting, Material Selection
\item \textbf{Prototyping \& Manufacturing:} FDM 3D Printing, Rapid Prototyping, CNC, Machining, Additive Manufacturing, Assembly Documentation
\item \textbf{Electronics Integration:} PCB Design (Eagle), Arduino, ESP32, STM32F4, Circuit Design, Sensors, I2C, SPI
\item \textbf{Programming:} C, C++, Python, MATLAB
\item \textbf{Testing \& Debugging:} Oscilloscope, Multimeter, FAT, System Integration
\item \textbf{Software:} SolidWorks Simulation, RSLogix, Visual Studio Code, Git, Google Suite, Microsoft Office
\item \textbf{Certifications:} Certified SolidWorks Associate, Bosch Rexroth (PLC, Sensors, Hydraulics)
\end{itemize}

\vspace{-10pt}
\section*{PROFESSIONAL EXPERIENCE}
\vspace{-5pt}
\hrule

\textbf{Research Assistant, Arizona State University, Tempe, AZ} \hfill 11/2023 - 5/2025
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Engineered 20+ precision parts in SolidWorks with in-house 3D printing, refining tolerances and materials to cut fit issues by 80\%.
\item Performed FEA to verify stiffness and strength, ensuring zero failures in high-load motion capture pods.
\item Built a laser-cut acrylic transport case using CAD and shop tools, cutting transport damage by 70\% and improving portability.
\item Produced 3D-print-ready models and managed in-house fabrication, reducing print-to-assembly time by 30\%.
\end{itemize}

\textbf{Junior Engineer R\&D, Next Big Innovation Labs Pvt. Ltd., Bengaluru, Karnataka, India} \hfill 7/2022 - 8/2023 
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Created coaxial/triaxial needle kits using GD\&T, delivering a 76\% cost reduction while maintaining precision and durability.
\item Fabricated a custom pellet-based extruder in India, lowering manufacturing cost by 66.8\% and enabling multi-material bioprinting.
\item Enhanced extruder heads through iterative prototyping and testing, raising operational efficiency by 15\%.
\item Partnered with electronics vendors to deliver tailored integration solutions, ensuring compatibility and long-term reliability.
\end{itemize}

\textbf{Intern, Nutron System Pvt. Ltd., Kalol, Gujarat, India} \hfill 4/2021 - 10/2021
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Automated PTFE Bellow assembly line with CAD and PLC, boosting efficiency by 40\% and reducing defects by 82\%.
\item Designed a glue dispenser and mesh cutter for Godrej \& Viega, improving automation and reducing manual work.
\item Collaborated with manufacturing teams to install automation systems, increasing uptime and reliability.
\end{itemize}

\vspace{-10pt}    
\section*{PROJECTS}
\vspace{-5pt}
\hrule

\textbf{All-Terrain Vehicle (Team)} \hfill May 2019 -- Feb 2020
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Designed suspension and steering systems in SolidWorks/Lotus to optimize durability and handling, securing 6th place nationally out of 80+ teams.
\item Developed custom sheet-metal and composite bodywork, improving aesthetics, ergonomics, and performance under competitive conditions.
\end{itemize}

\textbf{Underwater ROV (Team)} \hfill Dec 2021 -- May 2022
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Built a deep-sea ROV integrating BLDC motors, waterproof enclosures, RF communication, and microcontrollers, enabling real-time video and sample collection at depth.
\item Validated structural integrity and thermal stability under simulated deep-water conditions, ensuring reliable long-duration operation.
\end{itemize}

\textbf{Nurse Robot (Team)} \hfill Mar 2020 -- Jun 2020
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Designed assistive robot hardware for contactless patient care; selected and integrated sensors and PCBA layouts within compact housing.
\item Contributed to rapid prototyping and testing for functional validation during COVID-19 response.
\end{itemize}

\textbf{SACH Cotton Harvester (Solo)} \hfill Jul 2021 -- May 2022
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Developed a semi-automatic cotton harvester with robotic manipulator and IoT-based collection, improving small-scale farm productivity by 40\% in field trials.
\item Designed mechanical linkages and modular attachments for easy adaptation to varied farm layouts and crop conditions.
\end{itemize}

\textbf{FDM 3D Printer (Solo)} \hfill Nov 2021 -- Dec 2022
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Designed and assembled a 400x400x400 mm FDM printer with optimized frame rigidity, linear motion system, and custom PCB integration, achieving consistent print quality.
\item Tuned firmware parameters and thermal management systems to ensure repeatable precision across multi-hour print cycles.
\end{itemize}

\end{document}
""".strip()


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
    tex = escape_tex_specials(tex)

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
    # counts \item occurrences (rough but effective for mutation control)
    # count \item anywhere (LLMs sometimes inline \item after \begin{itemize})
    return len(re.findall(r"\\item\b", latex))

def require_same_section_markers(master: str, tailored: str) -> None:
    required = [
        r"\section*{SUMMARY}",
        r"\section*{EDUCATION}",
        r"\section*{TECHNICAL SKILLS}",
        r"\section*{PROFESSIONAL EXPERIENCE}",
        r"\section*{PROJECTS}",
    ]
    for m in required:
        if m not in tailored:
            raise RuntimeError(f"mutation_violation: missing section marker {m}")

    # ensure all required markers exist in master too (sanity)
    for m in required:
        if m not in master:
            raise RuntimeError(f"pipeline_error: master missing section marker {m}")

def require_no_new_companies(master: str, tailored: str) -> None:
    # simplest guard: tailored cannot contain bold employer lines not in master
    # compare all \textbf{...} lines (coarse but strong)
    master_bold = set(re.findall(r"\\textbf\{([^}]+)\}", master))
    tailored_bold = set(re.findall(r"\\textbf\{([^}]+)\}", tailored))
    # allow tailored subset, but forbid new bold entries (new roles/projects)
    new_bold = sorted([b for b in tailored_bold if b not in master_bold])
    if new_bold:
        raise RuntimeError("mutation_violation: introduced new \\textbf entries: " + ", ".join(new_bold[:10]))

def require_bullet_count_stable(
    master: str, tailored: str, max_drop: int = 2, max_add: int = 2
) -> None:
    m = count_itemize_items(master)
    t = count_itemize_items(tailored)
    if t < m - max_drop or t > m + max_add:
        raise RuntimeError(f"mutation_violation: bullet_count master={m} tailored={t}")


def compile_pdf(tex_path: pathlib.Path) -> pathlib.Path:
    out_dir = tex_path.parent
    try:
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
                master_latex=MASTER_LATEX,
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
            tailored_latex = merge_with_master_preamble(MASTER_LATEX, tailored_latex)

            ok, reason = looks_like_latex_resume(tailored_latex)
            # Mutation guards: fail fast if AI changed structure
            require_same_section_markers(MASTER_LATEX, tailored_latex)
            try:
                require_bullet_count_stable(
                    MASTER_LATEX, tailored_latex, max_drop=2, max_add=0
                )
            except RuntimeError:
                pack = generate_apply_pack(
                    master_latex=MASTER_LATEX,
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
                require_same_section_markers(MASTER_LATEX, tailored_latex)
                require_bullet_count_stable(
                    MASTER_LATEX, tailored_latex, max_drop=2, max_add=0
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

            tex_path = out_dir / "Poojan_Vanani_Resume.tex"
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
                    "Status": "Applied",
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

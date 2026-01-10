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
\documentclass[10pt]{extarticle}
\usepackage[left=0.4in,right=0.4in,top=0.4in,bottom=0.4in]{geometry}
\usepackage[hidelinks]{hyperref}
\usepackage{enumitem}
\usepackage{titlesec}
\usepackage{parskip}
\usepackage{xcolor}
\definecolor{mygreen}{HTML}{169137}
\definecolor{myblue}{HTML}{1848cc}

\pagenumbering{gobble}
\titleformat{\section}{\large\bfseries}{}{0em}{}
\titleformat{\subsection}{\bfseries}{}{0em}{}
\setlength{\parindent}{0pt}

\title{\color{myblue}{\vspace{-1.5cm} \LARGE \textbf{OM KIRANBHAI PATEL}}}
\date{}

\begin{document}

\maketitle
\vspace{-2cm}
\hrule
\begin{center}
+1 (480) 876-1813 \quad \href{mailto:ompatel0584@gmail.com}{ompatel0584@gmail.com} \quad \href{https://www.linkedin.com/in/om-patel-1512om/}{www.linkedin.com/in/om-patel-1512om} \quad \href{https://www.ompatel.info}{www.ompatel.info}
\end{center}

\hrule
\vspace{-1pt}
\textbf{B.S./M.S. (4+1) in Computer Science} \hfill Expected Dec 2026 \\
Arizona State University, Tempe, AZ \hfill GPA: 3.98 \\
\vspace{-18pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item \textbf{Relevant Coursework:} Machine Learning, AI, Cloud Computing, Data Structures \& Algorithms, Distributed Systems, Operating Systems, Web Development, Data Mining
\end{itemize}

\vspace{-10pt}
\section*{\color{mygreen}{SKILLS}}
\hrule
\begin{itemize}[left=5pt,itemsep=-4pt]
\vspace{-1pt}
\item \textbf{Languages:} Python, C, C\texttt{++}, Java, JavaScript, TypeScript, SQL
\item \textbf{AI/ML:} PyTorch, TensorFlow, Scikit-Learn, model evaluation, data preprocessing
\item \textbf{Backend:} Node.js, REST APIs, Flask, Django, Firebase
\item \textbf{Frontend:} React.js, Next.js, React Native, HTML/CSS
\item \textbf{Cloud/DevOps:} AWS, Kubernetes, Linux, Docker, Git, GitLab CI/CD
\end{itemize}

\vspace{-10pt}
\section*{\color{mygreen}{EXPERIENCE}}
\hrule
\vspace{-1pt}
\textbf{TheBeautyRunners}, Tempe, AZ \hfill Jan 2025 -- Dec 2025 \\
\textit{Software Engineer}
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Built and maintained a scalable \textbf{React Native} application with validated APIs, authentication, and async workflows for production use.
\item Implemented \textbf{serverless backend operations} using \textbf{Firebase} for authentication, data storage, and cloud functions; integrated \textbf{Stripe} for secure payment processing.
\item Deployed the mobile application to \textbf{Apple TestFlight} for beta testing, managing builds, versioning, and user feedbacks.
\end{itemize}

\textbf{Arizona State University}, Tempe, AZ \hfill Nov 2022 -- Dec 2025 \\
\textit{IT Support \& Content Manager}
\begin{itemize}[left=5pt,itemsep=-4pt]
\vspace{-4pt}
\item Managed and maintained operational data in \textbf{Salesforce}, supporting data accuracy, reporting, and business workflows.
\item Built automation and data-processing tools in \textbf{Python} to analyze records, track trends, and reduce manual effort.
\item Authored technical documentation and process guides used by cross-functional teams across the university.
\end{itemize}

\textbf{Three Martian IT Solutions}, Surat, India \hfill May 2022 -- Jul 2022 \\
\textit{Python Developer}
\begin{itemize}[left=5pt,itemsep=-4pt]
\vspace{-4pt}
\item Enhanced \textbf{Django} modules, improving system maintainability and API performance through optimized SQL and caching.
\item Implemented automated unit tests, improved backend reliability, and deployed containerized builds via Docker.
\item Documented API behavior, module dependencies, and code changes for long-term maintainability.
\end{itemize}

\vspace{-10pt}
\section*{\color{mygreen}{PROJECTS}}
\hrule
\vspace{-1pt}
\textbf{Preview Environment Manager}
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Engineered an automated \textbf{Kubernetes} preview system using \textbf{Node.js, Express, and GitHub webhooks} that dynamically provisions isolated namespaces per pull request, accelerating code review by \textbf{enabling instant deployment validation}.
\item Orchestrated \textbf{CI/CD automation with Docker, kubectl, and kind}, implementing secure webhook verification, resource-constrained deployments with health probes, and lifecycle management achieving \textbf{zero-touch infrastructure operations}.
\end{itemize}


\textbf{Serverless \& Edge-Based Face Recognition Pipeline}
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Architected a distributed \textbf{cloud + edge AI system} using \textbf{AWS Lambda, SQS, ECR, and IoT Greengrass} with \textbf{MTCNN + FaceNet models} for real-time facial recognition, achieving \textbf{100\% accuracy} under production workloads.
\item Optimized inference by offloading detection to \textbf{edge devices via MQTT}, containerizing PyTorch with CPU-optimized Docker builds, and implementing async message queuing that reduced latency by \textbf{40\%} across distributed environments.
\end{itemize}


\textbf{GetCoverly.ai}
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Developing an AI-powered resume and cover-letter generator using \textbf{React, Node.js, and Firebase} integrated with OpenAI, allowing users to craft tailored documents \textbf{3× faster}.
\item Applied \textbf{machine learning for job-skill mapping} to raise personalization accuracy and automate NLP-driven generation.
\end{itemize}

\textbf{GPU-Accelerated Binary Classifier}
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Designed a GPU-optimized binary classifier in Python using \textbf{NumPy/cuML}, enabling \textbf{3× faster} training on large datasets.
\item Evaluated model performance, tuned parameters, and analyzed class-imbalance effects to improve accuracy and reliability.
\end{itemize}

\textbf{AI-Enabled Mevent Console}
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Built real-time dashboards with \textbf{Next.js} and WebSockets for event monitoring and pattern detection.
\item Implemented accessibility, automated tests, and structured documentation to support long-term extensibility.
\end{itemize}

\textbf{Custom C\texttt{++} Compiler and Interpreter}
\vspace{-4pt}
\begin{itemize}[left=5pt,itemsep=-4pt]
\item Implemented compiler components (lexer, parser, IR interpreter) using core algorithmic techniques and memory-safe C\texttt{++}.
\item Added structured error reporting, test harnesses, and Linux scripts to ensure deterministic behavior and robust debugging.
\end{itemize}

\textbf{ConvertEase Discord Bot}
\begin{itemize}[left=5pt,itemsep=-4pt]
\vspace{-4pt}
\item Automated file processing and conversions using \textbf{Python}, including error-tolerant command parsing.
\item Reduced conversion times by 50\% through optimized algorithms and caching techniques.
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
    tex = tex.replace(r"\times", "x").replace(r"\pm", "+/-")

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
        r"\section*{\color{mygreen}{SKILLS}}",
        r"\section*{\color{mygreen}{EXPERIENCE}}",
        r"\section*{\color{mygreen}{PROJECTS}}",
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
    master: str, tailored: str, max_drop: int = 10, max_add: int = 10
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
                    MASTER_LATEX, tailored_latex, max_drop=10, max_add=10
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
                    MASTER_LATEX, tailored_latex, max_drop=10, max_add=10
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

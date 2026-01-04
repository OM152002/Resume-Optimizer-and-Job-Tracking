import os
import json
import pathlib
import subprocess
import uuid
from datetime import datetime, timezone

from .latex_validate import looks_like_latex_resume
from .llm_optional import tailor_resume
from .notion_client import (
    get_database_schema,
    build_property_index,
    fetch_by_status,
    update_page_safe,
)

ART_DIR = pathlib.Path("artifacts")
ART_DIR.mkdir(exist_ok=True)

# IMPORTANT: Replace this with your full master LaTeX template
MASTER_LATEX = r"""
\documentclass[10.5pt]{article}
\usepackage[margin=0.4in, bottom=0.4in, top=0.5in]{geometry}
\usepackage{hyperref}
\usepackage{enumitem}
\usepackage{titlesec}
\usepackage{parskip}
\usepackage{comment}

\pagenumbering{gobble}

% Define custom section font

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
\item Designed and assembled a 400×400×400 mm FDM printer with optimized frame rigidity, linear motion system, and custom PCB integration, achieving consistent print quality.
\item Tuned firmware parameters and thermal management systems to ensure repeatable precision across multi-hour print cycles.
\end{itemize}


\end{document}
""".strip()


def sh(cmd: list[str], cwd: str | None = None) -> str:
    r = subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=cwd)
    return (r.stdout or "").strip()


def safe_text(prop) -> str:
    """
    Read Notion text from title/rich_text.
    """
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
    """
    Return the first matching property object in props.
    Handles trailing spaces and case by checking common variants.
    """
    # direct checks
    for c in candidates:
        if c in props:
            return props[c]

    # tolerant checks: normalize keys
    def norm(x: str) -> str:
        return " ".join((x or "").strip().lower().split())

    props_norm = {norm(k): k for k in props.keys()}
    for c in candidates:
        nk = norm(c)
        if nk in props_norm:
            return props[props_norm[nk]]

    # trailing-space variants
    for c in candidates:
        for k in props.keys():
            if k.strip() == c.strip():
                return props[k]

    return None


def main():
    limit = int(os.getenv("LIMIT") or "5")

    run_id = uuid.uuid4().hex[:10]
    model_name = os.getenv("MODEL_NAME", "none")
    prompt_version = os.getenv("PROMPT_VERSION", "v1")

    remote = os.environ["RCLONE_REMOTE"]          # e.g. gdrive
    drive_root = os.environ.get("DRIVE_ROOT", "JobApps")

    schema = get_database_schema()
    idx = build_property_index(schema)

    # Trigger state
    items = fetch_by_status("Not Applied", limit=limit, idx=idx)

    run_log = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "run_id": run_id,
        "processed": 0,
        "ok": 0,
        "errors": 0,
        "details": []
    }

    for page in items:
        page_id = page["id"]
        job_id = page_id.replace("-", "")
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
            info = update_page_safe(page_id, {
                "Status": "Error",
                "Errors": "Job Description is empty or unreadable by API",
                "Run ID": run_id,
                "Model": model_name,
                "Prompt version": prompt_version,
            }, idx)

            run_log["errors"] += 1
            run_log["processed"] += 1
            run_log["details"].append({
                "page": page_id,
                "status": "error",
                "reason": "empty_jd",
                "notion": info
            })
            continue

        try:
            tailored = tailor_resume(MASTER_LATEX, jd)
            ok, reason = looks_like_latex_resume(tailored)

            if not ok:
                info = update_page_safe(page_id, {
                    "Status": "Error",
                    "Errors": f"LaTeX invalid: {reason}",
                    "Run ID": run_id,
                    "Model": model_name,
                    "Prompt version": prompt_version,
                }, idx)

                run_log["errors"] += 1
                run_log["processed"] += 1
                run_log["details"].append({
                    "page": page_id,
                    "status": "error",
                    "reason": reason,
                    "notion": info
                })
                continue

            # Local artifact folder per job (prevents overwrites, constant filename)
            out_dir = ART_DIR / f"{clean_path_segment(company)}_{clean_path_segment(role)}"
            out_dir.mkdir(parents=True, exist_ok=True)

            tex_path = out_dir / "Poojan_Vanani_Resume.tex"
            tex_path.write_text(tailored, encoding="utf-8")

            # Compile PDF (ATS-friendly)
            sh(["tectonic", tex_path.name, "--outdir", "."], cwd=str(out_dir))

            pdf_path = out_dir / "Poojan_Vanani_Resume.pdf"
            if not pdf_path.exists():
                raise RuntimeError("tectonic did not generate Poojan_Vanani_Resume.pdf")

            # Drive destination directory: JobApps/<Company>/<Role>/<pageid>
            dest_dir = f"{drive_root}/{clean_path_segment(company)}/{clean_path_segment(role)}/{job_id}"

            # Upload to Drive + get links
            sh(["rclone", "mkdir", f"{remote}:{dest_dir}"])
            sh(["rclone", "copyto", str(pdf_path), f"{remote}:{dest_dir}/{pdf_path.name}"])
            sh(["rclone", "copyto", str(tex_path), f"{remote}:{dest_dir}/{tex_path.name}"])

            pdf_link = sh(["rclone", "link", f"{remote}:{dest_dir}/{pdf_path.name}"])
            tex_link = sh(["rclone", "link", f"{remote}:{dest_dir}/{tex_path.name}"])

            # Update Notion with links (use your URL columns)
            info = update_page_safe(page_id, {
                "Status": "Applied",
                "Errors": "",
                "Run ID": run_id,
                "Model": model_name,
                "Prompt version": prompt_version,
                "Resume PDF": pdf_link,
                "Resume Latex": tex_link,
            }, idx)

            run_log["ok"] += 1
            run_log["processed"] += 1
            run_log["details"].append({
                "page": page_id,
                "status": "ok",
                "company": company,
                "role": role,
                "url": url,
                "tex": str(tex_path),
                "pdf": str(pdf_path),
                "drive_pdf": pdf_link,
                "drive_tex": tex_link,
                "notion": info
            })

        except Exception as e:
            info = update_page_safe(page_id, {
                "Status": "Error",
                "Errors": str(e)[:2000],
                "Run ID": run_id,
                "Model": model_name,
                "Prompt version": prompt_version,
            }, idx)

            run_log["errors"] += 1
            run_log["processed"] += 1
            run_log["details"].append({
                "page": page_id,
                "status": "error",
                "reason": str(e),
                "notion": info
            })

    (ART_DIR / "run_log.json").write_text(json.dumps(run_log, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

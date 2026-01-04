import os
import json
import pathlib
import uuid
from datetime import datetime, timezone

from .notion_client import fetch_queued, update_page
from .latex_validate import looks_like_latex_resume
from .llm_optional import tailor_resume

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

def safe_text(prop) -> str:
    try:
        rt = prop.get("rich_text") or prop.get("title") or []
        return "".join([x["plain_text"] for x in rt])
    except Exception:
        return ""

def main():
    limit = int(os.getenv("LIMIT") or "5")
    run_id = uuid.uuid4().hex[:10]
model_name = "none"
prompt_version = "v1"

    items = fetch_queued(limit=limit)

    run_log = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "processed": 0,
        "ok": 0,
        "errors": 0,
        "details": []
    }

    for page in items:
        page_id = page["id"]
        props = page.get("properties", {})

        company = safe_text(props.get("Company", {}))
        role = safe_text(props.get("Role", {}))
        url = (props.get("Job URL", {}) or {}).get("url") or ""
        jd = safe_text(props.get("Job Description", {}))

        try:
            tailored = tailor_resume(MASTER_LATEX, jd)
            ok, reason = looks_like_latex_resume(tailored)

            if not ok:
                update_page(page_id, {
    "Status": {"status": {"name": "Error"}},
    "Errors": {"rich_text": [{"text": {"content": str(e)[:2000]}}]},
})
                run_log["errors"] += 1
                run_log["details"].append({"page": page_id, "status": "error", "reason": reason})
                run_log["processed"] += 1
                continue

            update_page(page_id, {
    # Status is a Notion "Status" property, not Select:
    "Status": {"status": {"name": "Applied"}},

    # Your DB property names:
    "Latex": {"rich_text": [{"text": {"content": tailored[:2000]}}]},

    # Optional: write keyword coverage if you compute it later
    # "Keywork Coverage": {"number": keyword_coverage},

    # Optional: run metadata if these properties exist (they do in your screenshot)
    "Run ID": {"rich_text": [{"text": {"content": run_id}}]},
    "Model": {"rich_text": [{"text": {"content": model_name}}]},
    "Prompt version": {"rich_text": [{"text": {"content": prompt_version}}]},

    # Clear Errors if it exists
    "Errors": {"rich_text": [{"text": {"content": ""}}]},
})

            fname = f"{company}_{role}".replace(" ", "_")[:80] or page_id
            (ART_DIR / f"{fname}.tex").write_text(tailored, encoding="utf-8")

            run_log["ok"] += 1
            run_log["details"].append({"page": page_id, "status": "ok", "company": company, "role": role, "url": url})

        except Exception as e:
            update_page(page_id, {
                "Status": {"select": {"name": "Error"}},
            })
            run_log["errors"] += 1
            run_log["details"].append({"page": page_id, "status": "error", "reason": str(e)})

        run_log["processed"] += 1

    (ART_DIR / "run_log.json").write_text(json.dumps(run_log, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()

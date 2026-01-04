# src/llm_gemini.py
import os
import json
from typing import Dict, Any

from google import genai
from pydantic import BaseModel, Field


class Outreach(BaseModel):
    linkedin_connect_note: str = Field(..., description="<=250 chars, no em dashes")
    linkedin_message: str
    recruiter_email: str
    followup_7d: str
    followup_14d: str


class ApplyPack(BaseModel):
    tailored_latex: str
    fit_score: float = Field(..., ge=0, le=100)
    keyword_coverage: float = Field(..., ge=0, le=100)
    top_keywords: list[str] = Field(default_factory=list)
    missing_keywords: list[str] = Field(default_factory=list)
    outreach: Outreach


# 1) PUT YOUR PROMPT IN THIS BLOCK (keep it instruction-only)
PROMPT_INSTRUCTIONS = r"""

Context:
I am providing two documents:
- My current resume (in .latex format).
- A target job description (JD) for the role I am applying to.

Your task is to revise my resume so it is optimized for ATS and tailored to the target job.
Integrate relevant keywords and skills from the JD without making it look artificial or stuffed.

Instructions

Keyword Optimization:
- Carefully analyze the job description and extract the most important hard skills, technical terms, tools, certifications, and role-specific keywords.
- Naturally integrate these keywords throughout my resume especially in experience bullet points, summary, and skills section — while maintaining readability and authenticity.

Role Alignment:
- Identify responsibilities and achievements from my current resume that most closely match the target role.
- Rewrite bullet points to highlight quantifiable achievements, results, and leadership impact relevant to the new job.
- Reorder or reframe content so the most role-aligned experiences are emphasized.

ATS-Friendly Formatting:
- Avoid parsing pitfalls (no tables, text boxes, graphics, headers/footers with critical info).
- Use consistent bullet formatting and standard section headers.
- Place keywords in a way ATS will parse correctly (Skills + Experience).

Professional Voice & Impact:
- Use strong action verbs.
- Focus on measurable outcomes where possible.
- Avoid vague phrases.

Balance:
- Do not keyword-stuff. Must read smoothly.
- Keep concise (1 page if under 8 yrs).

Final Output:
- Deliver a complete revised resume.
- Include a summary tailored to the JD.
- End with a skills section listing core competencies aligned with the JD.

Deliverable:
Output the final revised resume ready to be copied back into LaTeX code.
""".strip()


# 2) THIS WRAPS YOUR PROMPT + INPUTS (DON'T EDIT KEYS)
PROMPT_TEMPLATE = r"""
You must output ONLY valid JSON that matches the given schema.
Do not output markdown. Do not output commentary.

Hard rules:
- Do not invent new claims (no new employers, awards, metrics, tools, degrees).
- Keep LaTeX ATS-friendly (no tables/graphics/columns).
- Preserve the LaTeX structure and packages.
- Ensure LaTeX compiles.

{instructions}

INPUTS
Company: {company}
Role: {role}
Job URL: {url}

JOB DESCRIPTION:
{jd}

MASTER RESUME LATEX (edit content but preserve structure):
{master}

REQUIRED JSON FIELDS:
- tailored_latex (string)
- fit_score (0-100)
- keyword_coverage (0-100)
- top_keywords (array)
- missing_keywords (array)
- outreach:
  - linkedin_connect_note (<=250 chars, no em dashes)
  - linkedin_message
  - recruiter_email
  - followup_7d
  - followup_14d
""".strip()


def generate_apply_pack(master_latex: str, jd: str, company: str, role: str, url: str) -> Dict[str, Any]:
    if not jd or not jd.strip():
        raise ValueError("empty_jd")

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    model = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

    prompt = PROMPT_TEMPLATE.format(
        instructions=PROMPT_INSTRUCTIONS,
        company=company or "",
        role=role or "",
        url=url or "",
        jd=jd.strip(),
        master=master_latex.strip(),
    )

    resp = client.models.generate_content(
        model=model,
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_schema": ApplyPack,
            "temperature": 0.2,
        },
    )

    data = getattr(resp, "parsed", None)
    if data is None:
        data = json.loads(resp.text)

    # pydantic -> dict
    if hasattr(data, "model_dump"):
        return data.model_dump()
    return dict(data)

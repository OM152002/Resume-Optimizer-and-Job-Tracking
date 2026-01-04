def looks_like_latex_resume(tex: str) -> tuple[bool, str]:
    if not tex:
        return False, "Empty output"
    must = ["\\documentclass", "\\begin{document}", "\\end{document}"]
    missing = [m for m in must if m not in tex]
    if missing:
        return False, f"Missing tokens: {missing}"
    if tex.count("{") != tex.count("}"):
        return False, "Unbalanced braces"
    return True, "OK"

#!/usr/bin/env python3
"""Convert the Week 7 report markdown to PDF."""

import markdown
from weasyprint import HTML
from pathlib import Path

REPORT_DIR = Path(__file__).parent
MD_FILE = REPORT_DIR / "Week7_Data_Contract_Enforcer_Report.md"
PDF_FILE = REPORT_DIR / "Week7_Data_Contract_Enforcer_Report.pdf"

CSS = """
@page { size: A4; margin: 2cm; }
body { font-family: 'DejaVu Sans', 'Liberation Sans', Arial, sans-serif; font-size: 11pt; line-height: 1.5; color: #1a1a1a; }
h1 { font-size: 22pt; border-bottom: 2px solid #2c3e50; padding-bottom: 8px; color: #2c3e50; }
h2 { font-size: 16pt; color: #2c3e50; margin-top: 24px; border-bottom: 1px solid #bdc3c7; padding-bottom: 4px; }
h3 { font-size: 13pt; color: #34495e; margin-top: 16px; }
table { border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 10pt; }
th { background-color: #2c3e50; color: white; padding: 8px 10px; text-align: left; }
td { padding: 6px 10px; border-bottom: 1px solid #ddd; }
tr:nth-child(even) { background-color: #f8f9fa; }
code { background-color: #f4f4f4; padding: 2px 4px; border-radius: 3px; font-size: 10pt; }
pre { background-color: #f4f4f4; padding: 12px; border-radius: 4px; overflow-x: auto; font-size: 9pt; line-height: 1.4; }
strong { color: #2c3e50; }
hr { border: none; border-top: 1px solid #ddd; margin: 20px 0; }
blockquote { border-left: 3px solid #2c3e50; margin: 10px 0; padding: 8px 16px; background: #f8f9fa; }
"""

md_text = MD_FILE.read_text()
html_body = markdown.markdown(md_text, extensions=["tables", "fenced_code"])
full_html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><style>{CSS}</style></head>
<body>{html_body}</body></html>"""

HTML(string=full_html).write_pdf(str(PDF_FILE))
print(f"PDF written to {PDF_FILE}")

"""Convertit un document Markdown en HTML paginé A4, prêt à imprimer en PDF.

Utilisé par export-evaluation.ps1 pour produire le livrable PDF de l'Exercice 1
à partir de la source Markdown versionnée.

Usage : python md2html.py <source.md> <sortie.html> "<Titre du document>"
"""

import sys
from pathlib import Path

import mistune

CSS = """
@page { size: A4; margin: 18mm 16mm 16mm 16mm; }
* { box-sizing: border-box; }
body {
  font-family: "Segoe UI", Calibri, Arial, sans-serif;
  font-size: 10.5pt; line-height: 1.5; color: #1f2937; margin: 0;
}
h1 { font-size: 19pt; color: #0f172a; margin: 0 0 14px; line-height: 1.25;
     border-bottom: 2.5px solid #ff9900; padding-bottom: 8px; }
h2 { font-size: 13pt; color: #b45309; margin: 22px 0 8px; page-break-after: avoid; }
h3 { font-size: 11.5pt; color: #334155; margin: 16px 0 6px; page-break-after: avoid; }
p  { margin: 0 0 9px; text-align: justify; }
ul { margin: 0 0 10px; padding-left: 20px; }
li { margin-bottom: 5px; }
strong { color: #0f172a; }
code { font-family: Consolas, "Courier New", monospace; font-size: 9.5pt;
       background: #f1f5f9; padding: 1px 4px; border-radius: 3px; }
a { color: #1d4ed8; text-decoration: none; }
blockquote {
  margin: 0 0 16px; padding: 10px 14px; background: #f8fafc;
  border-left: 3px solid #94a3b8; font-size: 9.5pt; color: #475569;
}
blockquote p { margin: 0 0 5px; text-align: left; }
blockquote p:last-child { margin-bottom: 0; }
table {
  width: 100%; border-collapse: collapse; margin: 0 0 14px;
  font-size: 9pt; page-break-inside: avoid;
}
th {
  background: #f1f5f9; color: #0f172a; text-align: left;
  padding: 6px 8px; border: 1px solid #cbd5e1; font-weight: 600;
}
td { padding: 6px 8px; border: 1px solid #e2e8f0; vertical-align: top; }
tr:nth-child(even) td { background: #fbfcfd; }
hr { border: 0; border-top: 1px solid #e2e8f0; margin: 18px 0; }
"""


def main() -> None:
    src, out, titre = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]
    rendu = mistune.create_markdown(plugins=["table", "strikethrough"])
    corps = rendu(src.read_text(encoding="utf-8"))
    out.write_text(
        f"<!doctype html><html lang=fr><head><meta charset='utf-8'>"
        f"<title>{titre}</title><style>{CSS}</style></head><body>{corps}</body></html>",
        encoding="utf-8",
    )
    print(f"{out.name} : {out.stat().st_size} octets")


if __name__ == "__main__":
    main()

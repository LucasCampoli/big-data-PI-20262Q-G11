"""Build one printable HTML from the delivery documents.

Run from the repo root: python scripts/build_entrega_html.py
Then open docs/entrega-1.html in a browser and print to PDF at A4.
"""
import base64, re, sys
from pathlib import Path
import markdown

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else ".")
DOCS = ROOT / "docs"
PARTS = [
    ("", DOCS / "design.md"),
    ("appendix", DOCS / "appendix.md"),
    ("decisions", ROOT / "DECISIONS.md"),
]

def inline_svg(html: str) -> str:
    """Embed architecture_v1.svg so the file prints standalone."""
    svg = (DOCS / "architecture_v1.svg").read_text()
    svg = re.sub(r'\swidth="100%"', '', svg, count=1)
    return re.sub(r'<img[^>]*architecture_v1\.svg[^>]*>',
                  f'<figure class="arch">{svg}</figure>', html, count=1)

def decisions_index_only(md: str) -> str:
    """Keep the header and the index table, drop the per-decision records.

    The body of the design already argues every decision where it is used, so the printed
    document would repeat itself. DECISIONS.md stays complete in the repo.
    """
    cut = md.index("\n## D1.")
    return md[:cut].rstrip() + (
        "\n\nCada registro, con su contexto, las alternativas que miramos y las consecuencias, está"
        "\nen `DECISIONS.md` en el repositorio. Acá va solo el índice, porque el cuerpo del diseño ya"
        "\nargumenta cada decisión donde se usa.\n"
    )


body = []
for anchor, path in PARTS:
    md = path.read_text()
    if path.name == "DECISIONS.md":
        md = decisions_index_only(md)
    # strip the mermaid source block, the SVG above it is what prints
    md = re.sub(r"```mermaid.*?```", "", md, flags=re.S)
    html = markdown.markdown(md, extensions=["tables", "fenced_code", "toc", "attr_list"])
    body.append(f'<section id="{anchor}">{html}</section>' if anchor else f"<section>{html}</section>")

html = f"""<!doctype html>
<meta charset="utf-8">
<title>Cloud Provider Analytics, entrega 1, grupo 11</title>
<style>
  @page {{ size: A4; margin: 16mm 15mm; }}
  html {{ font-size: 10pt; }}
  body {{ font-family: Georgia, "Times New Roman", serif; line-height: 1.34; color: #1a1a1a;
         max-width: 195mm; margin: 0 auto; }}
  h1 {{ font-size: 1.7rem; margin: 0 0 .3rem; }}
  h2 {{ font-size: 1.2rem; margin: 1.1rem 0 .35rem; border-bottom: 1px solid #ccc;
        padding-bottom: .15rem; page-break-after: avoid; }}
  h3 {{ font-size: 1rem; margin: .75rem 0 .25rem; page-break-after: avoid; }}
  h4 {{ font-size: .98rem; margin: .8rem 0 .25rem; page-break-after: avoid; }}
  p, li {{ orphans: 3; widows: 3; }}
  table {{ border-collapse: collapse; width: 100%; margin: .4rem 0 .7rem;
           font-family: "Helvetica Neue", Arial, sans-serif; font-size: .74rem;
           page-break-inside: avoid; }}
  th, td {{ border: 1px solid #bbb; padding: 3px 5px; text-align: left; vertical-align: top; }}
  th {{ background: #f0f0f0; font-weight: 600; }}
  code {{ font-family: "DejaVu Sans Mono", Consolas, monospace; font-size: .8em;
          background: #f4f4f4; padding: 0 2px; }}
  pre {{ background: #f7f7f7; border: 1px solid #ddd; padding: 5px 7px; overflow-x: auto;
         font-size: .85rem; line-height: 1.3; page-break-inside: avoid; }}
  pre code {{ background: none; padding: 0; }}
  figure.arch {{ margin: 0; text-align: center; page-break-inside: avoid;
                 page-break-before: always; page-break-after: always; }}
  figure.arch svg {{ max-height: 258mm; max-width: 100%; height: auto; width: auto; }}
  section + section {{ page-break-before: always; }}
  a {{ color: #11457a; text-decoration: none; }}
</style>
{inline_svg("".join(body))}
"""
out = DOCS / "entrega-1.html"
out.write_text(html)
print(f"wrote {out} ({len(html)//1024} KB)")

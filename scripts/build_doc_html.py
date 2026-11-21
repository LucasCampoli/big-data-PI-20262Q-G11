"""Build the printable HTML of the final document: docs/final.md, the governance matrix, the
execution sheet and the index of decisions, with the architecture figure embedded.

python scripts/build_doc_html.py      ->  docs/final.html

Print it to PDF at A4 from a browser (background graphics on, no headers or footers), or headless:
chrome --headless --no-pdf-header-footer --print-to-pdf=docs/final.pdf docs/final.html
"""
import re
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
SVG = "architecture_final.svg"
PARTS = [("", DOCS / "final.md"), ("governance", DOCS / "governance.md"),
         ("execution", DOCS / "execution_sheet.md"), ("decisions", ROOT / "DECISIONS.md")]
# §2 starts on a new page with its title, figure and text together; the annexes follow each other.
FIGURE_CSS = ("figure.arch { page-break-before: auto; page-break-after: auto; } "
              "figure.arch svg { max-height: 200mm; } "
              'h2[id="2-arquitectura-final"] { page-break-before: always; } '
              "section#execution, section#decisions { page-break-before: auto; margin-top: 1.4rem; }")


def inline_svg(html: str) -> str:
    """Embed the architecture SVG so the file prints standalone."""
    svg = re.sub(r'\swidth="100%"', "", (DOCS / SVG).read_text(), count=1)
    return re.sub(rf"<img[^>]*{re.escape(SVG)}[^>]*>", f'<figure class="arch">{svg}</figure>', html, count=1)


def decisions_index_only(md: str) -> str:
    """The document argues each decision where it is used; the printout keeps only the index."""
    return md[:md.index("\n## D1.")].rstrip() + (
        "\n\nCada registro, con su contexto, las alternativas que miramos y las consecuencias, está"
        "\nen `DECISIONS.md` en el repositorio.\n")


def main():
    body = []
    for anchor, path in PARTS:
        md = path.read_text()
        if path.name == "DECISIONS.md":
            md = decisions_index_only(md)
        html = markdown.markdown(md, extensions=["tables", "fenced_code", "toc", "attr_list"])
        body.append(f'<section id="{anchor}">{html}</section>' if anchor else f"<section>{html}</section>")
    figure_css = FIGURE_CSS
    html = f"""<!doctype html>
<meta charset="utf-8">
<title>Cloud Provider Analytics, MVP final, grupo 11</title>
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
  {figure_css}
</style>
{inline_svg("".join(body))}
"""
    out = DOCS / "final.html"
    out.write_text(html)
    print(f"wrote {out.relative_to(ROOT)} ({len(html) // 1024} KB)")


if __name__ == "__main__":
    main()

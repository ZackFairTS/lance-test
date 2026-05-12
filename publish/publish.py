#!/usr/bin/env python3
"""Render a markdown file to HTML (lance-docs style) and publish to S3+CloudFront.

Usage:
  python3 publish.py <markdown_path> [options]

Example:
  python3 publish.py ~/lance-extended-bench/docs/COMPOSITE_KEY_INDEX.md \
      --slug composite-key-index \
      --title 'Lance Table 组合字段查询方案设计' \
      --subtitle 'video_id + frame_id 类组合查询的证据驱动决策' \
      --badge 'lance-extended-bench' \
      --badge 'ai-slop-remover reviewed:pass' \
      --badge 'pylance 4.0.1' \
      --related 'partitioning-design.html:video_id 分区方案（Pattern D）' \
      --publish
"""
import argparse
import datetime
import html
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

import markdown
from pygments.formatters import HtmlFormatter

sys.path.insert(0, str(Path(__file__).parent))
from themes import render_theme_vars

S3_BUCKET = "lance-docs-113343415039-ap-northeast-1"
CLOUDFRONT_ID = "EN60406PGR72M"
CDN_DOMAIN = "d3c24sti1doqj9.cloudfront.net"

TEMPLATE_PATH = Path(__file__).parent / "template.html"

MD_EXTENSIONS = [
    "extra",
    "toc",
    "codehilite",
    "tables",
    "fenced_code",
    "sane_lists",
]
MD_EXT_CONFIGS = {
    "toc": {"toc_depth": "2-4"},
    "codehilite": {"css_class": "codehilite", "guess_lang": False},
}


def render_badges(badges: List[str]) -> str:
    parts = []
    for b in badges:
        if ":" in b:
            text, cls = b.split(":", 1)
            parts.append(f'<span class="badge {html.escape(cls)}">{html.escape(text)}</span>')
        else:
            parts.append(f'<span class="badge">{html.escape(b)}</span>')
    return "\n      ".join(parts)


def render_related(related: Optional[str]) -> str:
    if not related:
        return ""
    if ":" not in related:
        raise SystemExit(f"--related must be 'url:title', got: {related!r}")
    url, title = related.split(":", 1)
    return f' · 关联文档：<a href="{html.escape(url)}">{html.escape(title)}</a>'


def strip_sections(md_text: str, needles: List[str]) -> str:
    if not needles:
        return md_text
    lines = md_text.splitlines(keepends=True)
    drop_from = None
    drop_heading_level = None
    kept = []
    for line in lines:
        m = re.match(r"^(#{1,6})\s+(.*?)\s*$", line)
        if m:
            level = len(m.group(1))
            text = m.group(2)
            if drop_from is not None and level <= drop_heading_level:
                drop_from = None
                drop_heading_level = None
            if drop_from is None and any(n in text for n in needles):
                drop_from = text
                drop_heading_level = level
                continue
        if drop_from is None:
            kept.append(line)
    return "".join(kept)


def render_html(md_text: str, args) -> str:
    md_text = strip_sections(md_text, args.strip_from)
    md = markdown.Markdown(extensions=MD_EXTENSIONS, extension_configs=MD_EXT_CONFIGS)
    body = md.convert(md_text)
    toc = md.toc
    codehilite_css = HtmlFormatter(style="default").get_style_defs(".codehilite")
    template = TEMPLATE_PATH.read_text(encoding="utf-8")
    subtitle_html = (
        f'<span class="subtitle">{html.escape(args.subtitle)}</span>'
        if args.subtitle
        else ""
    )
    replacements = {
        "{{TITLE}}": html.escape(args.title or args.slug),
        "{{THEME_VARS}}": render_theme_vars(args.theme),
        "{{CODEHILITE_CSS}}": codehilite_css,
        "{{BADGES}}": render_badges(args.badge),
        "{{H1_TITLE}}": html.escape(args.title or args.slug),
        "{{H1_SUBTITLE}}": subtitle_html,
        "{{TOC}}": toc,
        "{{CONTENT}}": body,
        "{{SOURCE_NAME}}": html.escape(Path(args.markdown).name),
        "{{RENDER_DATE}}": datetime.date.today().isoformat(),
        "{{RELATED_LINK}}": render_related(args.related),
    }
    out = template
    for k, v in replacements.items():
        out = out.replace(k, v)
    return out


def publish_to_s3(local_html: Path, slug: str) -> dict:
    key = f"{slug}.html"
    cp_cmd = [
        "aws", "s3", "cp", str(local_html),
        f"s3://{S3_BUCKET}/{key}",
        "--content-type", "text/html; charset=utf-8",
        "--cache-control", "public, max-age=300",
    ]
    cp = subprocess.run(cp_cmd, capture_output=True, text=True)
    if cp.returncode != 0:
        raise SystemExit(f"S3 upload failed:\nSTDERR: {cp.stderr}")
    inv_cmd = [
        "aws", "cloudfront", "create-invalidation",
        "--distribution-id", CLOUDFRONT_ID,
        "--paths", f"/{key}",
        "--output", "json",
    ]
    inv = subprocess.run(inv_cmd, capture_output=True, text=True)
    if inv.returncode != 0:
        raise SystemExit(f"Invalidation failed:\nSTDERR: {inv.stderr}")
    inv_data = json.loads(inv.stdout)
    return {
        "s3_url": f"s3://{S3_BUCKET}/{key}",
        "cdn_url": f"https://{CDN_DOMAIN}/{key}",
        "invalidation_id": inv_data["Invalidation"]["Id"],
    }


def slugify(path: Path) -> str:
    stem = path.stem.lower()
    slug = re.sub(r"[^a-z0-9]+", "-", stem).strip("-")
    return slug or "document"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("markdown", help="path to source .md")
    p.add_argument("--slug", help="output filename stem (default: derived from markdown filename)")
    p.add_argument("--title", help="browser tab + h1 title (default: slug)")
    p.add_argument("--subtitle", help="h1 subtitle line")
    p.add_argument("--badge", action="append", default=[],
                   help="badge text, optionally with ':pass'/':fail'/':warn' variant, repeatable")
    p.add_argument("--theme", default="light", help="theme name (see themes.py)")
    p.add_argument("--related", help="footer related-doc link, format 'url:title'")
    p.add_argument("--output", help="local output path (default: /tmp/<slug>.html)")
    p.add_argument("--strip-from", action="append", default=[],
                   help="strip markdown from any heading matching this text to EOF (or next heading "
                        "of equal-or-higher level), repeatable. Matches by substring on heading text.")
    p.add_argument("--publish", action="store_true", help="upload to S3 and invalidate CloudFront")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    md_path = Path(args.markdown).expanduser().resolve()
    if not md_path.is_file():
        raise SystemExit(f"Not a file: {md_path}")
    if not args.slug:
        args.slug = slugify(md_path)
    out_html = render_html(md_path.read_text(encoding="utf-8"), args)
    output_path = Path(args.output) if args.output else Path(f"/tmp/{args.slug}.html")
    output_path.write_text(out_html, encoding="utf-8")
    print(f"✓ Rendered: {output_path} ({len(out_html):,} bytes)")
    if args.publish:
        result = publish_to_s3(output_path, args.slug)
        print(f"✓ Uploaded: {result['s3_url']}")
        print(f"✓ Invalidation: {result['invalidation_id']}")
        print(f"✓ CDN URL:   {result['cdn_url']}")
    else:
        print("  (not published; pass --publish to upload)")


if __name__ == "__main__":
    main()

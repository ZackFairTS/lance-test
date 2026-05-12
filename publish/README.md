# lance-docs-site publish pipeline

Markdown → HTML → S3 → CloudFront, scripted. No `mkdocs`, no GitHub Actions —
runs on this EMR master. Takes ~2 seconds.

## Layout

```
/mnt/tmp/lance-docs-site/
├── index.html          # landing page (hand-written, dark theme)
└── publish/
    ├── publish.py      # main entrypoint — renders + uploads + invalidates
    ├── themes.py       # CSS variable sets (light / dark)
    ├── template.html   # HTML scaffold with {{PLACEHOLDERS}}
    └── README.md       # this file
```

## Quick start

Render **and** publish `composite-key-index.html`:

```bash
python3 /mnt/tmp/lance-docs-site/publish/publish.py \
  ~/lance-extended-bench/docs/COMPOSITE_KEY_INDEX.md \
  --slug composite-key-index \
  --title 'Lance Table 组合字段查询方案设计' \
  --subtitle 'video_id + frame_id 类组合查询的证据驱动决策' \
  --badge 'lance-extended-bench' \
  --badge 'ai-slop-remover reviewed:pass' \
  --badge 'pylance 4.0.1' \
  --related 'partitioning-design.html:video_id 分区方案（Pattern D）' \
  --publish
```

Render only (dry run to `/tmp/<slug>.html`): omit `--publish`.

## Flags

| Flag | Effect |
|---|---|
| `markdown` (positional) | Source `.md` file |
| `--slug` | Output filename stem (default: derived from md filename) |
| `--title` | Browser tab + H1 title |
| `--subtitle` | Sub-heading under H1 |
| `--badge TEXT` | Badge, repeatable. Append `:pass` / `:fail` / `:warn` for colored variant |
| `--theme {light,dark}` | Theme name (default: `light`) |
| `--related 'url:title'` | Footer "related docs" link |
| `--strip-from TEXT` | Omit any heading whose text contains TEXT from published HTML (and everything under it, until the next equal-or-higher heading). Source `.md` is not modified. Repeatable. Common use: `--strip-from '修订记录'` to keep internal revision history out of the public HTML while preserving it in the source file. |
| `--output PATH` | Local output path (default: `/tmp/<slug>.html`) |
| `--publish` | Upload to S3 + CloudFront invalidation. Without this, just renders locally |

## AWS resources

| What | ID |
|---|---|
| S3 bucket | `lance-docs-113343415039-ap-northeast-1` |
| CloudFront distribution | `EN60406PGR72M` |
| Public URL | `https://d3c24sti1doqj9.cloudfront.net/<slug>.html` |

IAM for this EMR master already has `s3:PutObject` and
`cloudfront:CreateInvalidation` on the above — confirmed working
2026-05-12.

## Adding a new document

1. Write `~/lance-<project>/docs/<NAME>.md`
2. Run `publish.py <md_path> --slug <slug> --title '<title>' --publish`
3. If you want it linked from `index.html`, edit `index.html` and re-upload:
   `aws s3 cp index.html s3://lance-docs-113343415039-ap-northeast-1/ --content-type "text/html; charset=utf-8"`
   `aws cloudfront create-invalidation --distribution-id EN60406PGR72M --paths /index.html`

## Adding a theme

Edit `themes.py`. Each theme is a dict of CSS custom properties that fill the
`:root` block in `template.html`. The existing `light` and `dark` variants are
anchored to the byte-exact values of the originally hand-written HTML pages
(see the comments in `themes.py`) — do not paraphrase those values unless you
intend the whole site's color scheme to shift.

## Why this is the way it is

Before this script existed, every doc update meant asking an LLM to read the
markdown, hand-write HTML matching an existing page's CSS, and manually
`aws s3 cp` + `create-invalidation`. That produced subtle bugs (e.g. the
2026-05-12 §3.1 timeline where the LLM stuffed a bullet list inside a `<p>`
tag instead of a `<ul>`).

`publish.py` replaces the hand-write step with `python-markdown` + `pygments`.
Output is deterministic; re-running it produces byte-identical HTML.

## Regenerating the existing pages

```bash
# composite-key-index (today's active page)
python3 publish.py ~/lance-extended-bench/docs/COMPOSITE_KEY_INDEX.md \
  --slug composite-key-index \
  --title 'Lance Table 组合字段查询方案设计' \
  --subtitle 'video_id + frame_id 类组合查询的证据驱动决策' \
  --badge 'lance-extended-bench' --badge 'pylance 4.0.1' \
  --related 'partitioning-design.html:video_id 分区方案（Pattern D）' \
  --strip-from '修订记录' \
  --publish

# partitioning-design (Pattern D design doc)
python3 publish.py ~/lance-extended-bench/docs/PARTITIONING_DESIGN.md \
  --slug partitioning-design \
  --title 'video_id 分区方案（Pattern D）' \
  --subtitle 'PB 级视频帧存储的 Lance namespace 分区设计' \
  --badge 'lance-extended-bench' --badge 'pylance 4.0.1' \
  --related 'composite-key-index.html:组合字段查询方案' \
  --strip-from 'Review Log' --strip-from '修订记录' \
  --publish
```

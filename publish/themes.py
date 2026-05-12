THEMES = {
    "light": {
        # Anchor values: extracted from composite-key-index.html (GitHub light style).
        # Do not paraphrase — they must stay byte-identical to the originally
        # published page so diffs stay small when migrating to publish.py.
        "--bg": "#ffffff",
        "--bg-card": "#f6f8fa",
        "--bg-code": "#f6f8fa",
        "--fg": "#1f2328",
        "--fg-muted": "#59636e",
        "--accent": "#0969da",
        "--accent-strong": "#0550ae",
        "--pass": "#1a7f37",
        "--fail": "#cf222e",
        "--warn": "#9a6700",
        "--na": "#6b7280",
        "--border": "#d1d9e0",
        "--kbd": "#afb8c1",
        "--inline-code-bg": "rgba(9, 105, 218, 0.08)",
        "--inline-code-fg": "#0550ae",
        "--inline-code-border": "rgba(9, 105, 218, 0.18)",
        "--th-bg": "rgba(96, 165, 250, 0.08)",
        "--quote-bg": "rgba(9, 105, 218, 0.06)",
    },
    "dark": {
        # Anchor values: extracted from /mnt/tmp/lance-docs-site/index.html
        # (Lance audit report dark style).
        "--bg": "#0f1419",
        "--bg-card": "#1a1f2e",
        "--bg-code": "#0a0e14",
        "--fg": "#d9e0e8",
        "--fg-muted": "#8a95a5",
        "--accent": "#60a5fa",
        "--accent-strong": "#3b82f6",
        "--pass": "#22c55e",
        "--fail": "#ef4444",
        "--warn": "#f59e0b",
        "--na": "#6b7280",
        "--border": "#2a3142",
        "--kbd": "#374151",
        "--inline-code-bg": "rgba(96, 165, 250, 0.12)",
        "--inline-code-fg": "#93c5fd",
        "--inline-code-border": "rgba(96, 165, 250, 0.22)",
        "--th-bg": "rgba(96, 165, 250, 0.08)",
        "--quote-bg": "rgba(96, 165, 250, 0.06)",
    },
}


def render_theme_vars(theme_name: str) -> str:
    if theme_name not in THEMES:
        available = ", ".join(THEMES)
        raise ValueError(f"Unknown theme '{theme_name}'; available: {available}")
    return "\n".join(f"  {k}: {v};" for k, v in THEMES[theme_name].items())

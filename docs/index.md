# OFS Skill Assessment — developer docs

This site is for **developers** working on the `ofs_skill` package: how to
contribute, how the package is organized, and **API reference generated from
Python docstrings**.

## User / theoretical docs → wiki

Install guides, configuration, how to run 1D/2D/ice skill, outputs, concepts,
and theoretical overview live in the
[project wiki](https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/wiki)
— **not** in this MkDocs site. Do not duplicate those guides under `docs/`.

| Audience | Where |
|----------|--------|
| Users / scientists (how to run, interpret outputs) | [Wiki](https://github.com/NOAA-CO-OPS/dev-Next-Gen-NOS-OFS-Skill-Assessment/wiki) |
| Developers (API, architecture, contributing) | This MkDocs site (GitHub Pages after merge to `main`) |

## How API pages are generated

API pages under [API](api/index.md) use
[mkdocstrings](https://mkdocstrings.github.io/). Each page contains directives
like:

```markdown
::: ofs_skill.obs_retrieval
```

At build time, MkDocs reads **docstrings and signatures** from `src/ofs_skill/`
(via Griffe). Prefer **Google-style** docstrings (`Args` / `Returns` /
`Raises` / `Attributes`) for public APIs — that matches `mkdocs.yml`. Improving
docs means improving the code’s docstrings, not copying wiki text into Markdown.

```bash
pip install -e ".[docs]"
mkdocs serve          # local preview
mkdocs build --strict # same gate as CI
```

## Start here

- [Architecture](architecture.md) — package map
- [Contributing](development/contributing.md) — setup, CI, test markers
- [API](api/index.md) — auto-generated reference
- [Migration guide](development/migration-guide.md) — calling the package from your code
- [Hand-written API notes](development/api-reference.md) — transitional overview (prefer the generated API)

## Docs gate

Pull requests run `mkdocs build --strict` in CI. Broken links, missing nav pages,
or unresolved `:::` targets fail the build.

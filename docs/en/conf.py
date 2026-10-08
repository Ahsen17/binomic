import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2] / "src"))

project = "Binomic"
author = "Ahsen17"
release = "0.1.1"

# Root URL of the published site, set by the Pages workflow so that
# cross-language links and Open Graph URLs stay absolute under the
# project subpath; empty for local preview served at "/".
root_url = os.environ.get("DOCS_ROOT_URL", "")

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
    "sphinxext.opengraph",
    "sphinx_tippy",
    "sphinx.ext.autodoc",
]

html_theme = "shibuya"

html_theme_options = {
    "github_url": "https://github.com/Ahsen17/binomic",
}

# Shibuya language switcher: each entry links the mirrored page in the
# Chinese tree; the %s placeholder receives the current page name.
html_context = {
    "languages": [
        ("简体中文", f"{root_url}/%s.html", "zh-CN"),
    ],
}

ogp_site_url = root_url

myst_enable_extensions = ["colon_fence", "deflist", "tasklist"]

source_suffix = {".rst": "restructuredtext", ".md": "markdown"}

exclude_patterns = [
    "_build",
]

# Mermaid
mermaid_parallel = True
mermaid_parallel_processes = 2

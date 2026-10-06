import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

project = "Binomic"
author = "ahsen17"
release = "0.1.0"
language = "zh_CN"

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
# English tree; the %s placeholder receives the current page name.
html_context = {
    "languages": [
        ("English", f"{root_url}/en/%s.html", "en"),
    ],
}

ogp_site_url = root_url

myst_enable_extensions = ["colon_fence", "deflist", "tasklist"]

source_suffix = {".rst": "restructuredtext", ".md": "markdown"}

# The English tree under en/ is a separate Sphinx source with its own
# conf.py and is built independently.
exclude_patterns = [
    "_build",
    "en",
    "en/*",
]

# Mermaid
mermaid_parallel = True
mermaid_parallel_processes = 2

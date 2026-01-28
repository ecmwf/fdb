# Configuration file for the Sphinx documentation builder.
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# NOTE: This configuration uses autoapi without automatic generation and
# instead uses directives (see api.rst). This is done because with just
# autoapi auto generation all modules must be imported. In ource case this
# would mean we need to complie our python bindings. Using autoapi with
# directives allows to use 'static analysis' mode to extract docstrings.default_mod
# See: https://sphinx-autoapi.readthedocs.io/en/stable/how_to.html#how-to-transition-to-autodoc-style-documentation
# See: https://sphinx-autoapi.readthedocs.io/en/stable/reference/directives.html

import datetime

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


project = "z3FDB"
copyright = f"{datetime.datetime.today().year}, ECMWF"
author = "ECMWF"
version = "local-dev"
release = "local-dev"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "sphinx.ext.autosectionlabel",
    "sphinxcontrib.mermaid",
    "autoapi.extension",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
]

# -- Autoapi settings
autoapi_dirs = ["../../src/z3fdb", "../../src/pychunked_data_view"]
autoapi_type = "python"
autoapi_generate_api_docs = False
autoapi_add_toctree_entry = False
autoapi_python_class_content = "class"
autoapi_ignore = [
    "*/_internal/*",
]
add_module_names = False

# -- Napoleon settings
napoleon_google = True

templates_path = ["_templates"]
exclude_patterns = ["Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
# Do not show source link on each page
html_show_sourcelink = False
html_sidebars = {
    "**": [],
}
html_theme_options = {
    "navbar_align": "left",
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["navbar-icon-links", "theme-switcher", "version-switcher"],
    "navbar_persistent": ["search-button"],
    "primary_sidebar_end": [],
    # On local builds no version.json is present
    "check_switcher": False,
}

html_context = {
    # Enable auto detection of light/dark mode
    "default_mode": "auto"
}

# -- autosectionlabel configuration ------------------------------------------
autosectionlabel_prefix_document = True

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import datetime
# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information


project = "Fields DataBase - FDB"
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
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.inheritance_diagram",
    "breathe",
    "sphinx.ext.autosectionlabel",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store", "_internal"]

autoapi_dirs = ["../src/pyfdb", "../src/z3fdb", "../src/pychunked_data_view"]
autoapi_type = "python"
autoapi_generate_api_docs = True
autoapi_add_toctree_entry = False
autoapi_python_class_content = "both"
autoapi_ignore = [
    "*/_internal/*",
]
add_module_names = False
autoapi_keep_files = False

# -- Napoleon settings
napoleon_google = True

templates_path = ["_templates"]
exclude_patterns = ["Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "pydata_sphinx_theme"
html_context = {"default_mode": "auto"}
html_theme_options = {
    "switcher": {
        "json_url": "https://sites.ecmwf.int/docs/dev-section/fdb/versions.json",
        "version_match": version,
    },
    "show_toc_level": 2,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/ecmwf/fdb",
            "icon": "fa-brands fa-github",
        },
    ],
    "navbar_align": "left",
    "navbar_start": ["navbar-logo"],
    "navbar_center": ["navbar-nav"],
    "navbar_end": ["navbar-icon-links", "theme-switcher", "version-switcher"],
    "navbar_persistent": ["search-button"],
    "secondary_sidebar_items": ["page-toc", "edit-this-page", "sourcelink"],
    # On local builds no version.json is present
    "check_switcher": False,
    "content_footer_items": [],
    "footer_start": ["copyright"],
    "footer_center": [],
    "footer_end": [],
}
html_sidebars = {"**": ["sidebar-nav-bs"]}
html_static_path = ["_static"]

# -- Breathe configuration ---------------------------------------------------
breathe_default_project = "FDB"

# -- autosectionlabel configuration ------------------------------------------
autosectionlabel_prefix_document = True

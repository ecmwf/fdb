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

extensions = ["breathe", "sphinx.ext.autosectionlabel"]

templates_path = ["_templates"]
exclude_patterns = ["Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "sphinx_book_theme"
html_context = {"default_mode": "default"}
html_theme_options = {
    "repository_url": "https://github.com/ecmwf/fdb",
    "repository_branch": "master",
    "use_source_button": True,
    "use_issues_button": True,
    "home_page_in_toc": False,
    "use_fullscreen_button": False,
    "use_download_button": False,
    "switcher": {
        "json_url": "https://sites.ecmwf.int/docs/dev-section/fdb/versions.json",
        "version_match": version,
    },
    "check_switcher": False,
    "primary_sidebar_end": ["version-switcher", "copyright"],
    "show_toc_level": 2,
    "icon_links": [
        {
            "name": "GitHub",
            "url": "https://github.com/ecmwf/fdb",
            "icon": "fa-brands fa-github",
        },
    ],
    "article_header_end": ["search-button", "toggle-secondary-sidebar"],
    "navbar_persistent": ["search-button"],
    "content_footer_items" : [],
    "footer_start": ["copyright"],
    "footer_center": [],
    "footer_end": [],
}
html_sidebars = {"**": ["icon-links", "search-field", "sbt-sidebar-nav.html"]}
html_static_path = ["_static"]

# -- Breathe configuration ---------------------------------------------------
breathe_default_project = "FDB"

# -- autosectionlabel configuration ------------------------------------------
autosectionlabel_prefix_document = True

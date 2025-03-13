# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "Fields DataBase - FDB"
copyright = "2021- ECMWF"
author = "ECMWF"
version = "local-dev"
release = "local-dev"

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = ["breathe"]

templates_path = ["_templates"]
exclude_patterns = ["Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "alabaster"
html_sidebars = {
    "**": [
        "about.html",
        "searchfield.html",
        "navigation.html",
    ]
}
html_theme_options = {
    "github_button": True,
    "github_user": "ecmwf",
    "github_repo": "fdb",
}
html_static_path = ["_static"]

# -- Breathe configuration ---------------------------------------------------
breathe_default_project = "FDB"

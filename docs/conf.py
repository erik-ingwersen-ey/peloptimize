"""Sphinx configuration."""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
import importlib

try:
    import sphinx.ext.imgmath  # noqa
except ImportError:
    img_ext = 'sphinx.ext.pngmath'
else:
    img_ext = 'sphinx.ext.imgmath'

load_dotenv(find_dotenv())
package_dir = os.environ.get('PACKAGE_DIRECTORY', None)
if package_dir is None:
    project_name = "peloptimize"
    max_parents = 2
    package_dir = Path('./').resolve()
    while package_dir.name != project_name and max_parents > 0:
        package_dir = package_dir.parent
        max_parents -= 1
else:
    package_dir = Path(package_dir).resolve()
package_dir = package_dir.joinpath('src')
sys.path.insert(0, str(package_dir))

package_name = "pelopt"
my_package = importlib.import_module(package_name)

version = getattr(my_package, '__version__')  # noqa
numpydoc_xref_param_type = True
numpydoc_xref_ignore = {'optional', 'type_without_description', 'BadException'}

project = "Peloptimize"
author = "Erik Ingwersen"
project_copyright = "2023, Erik Ingwersen"

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The master toctree document.
master_doc = 'index'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']

extensions = [
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx_click",
    "myst_parser",
    'sphinx.ext.ifconfig',
    'sphinx.ext.viewcode',
    'numpydoc',
    "sphinx_inline_tabs",
    img_ext,
]
autodoc_typehints = "description"

# -- Options for HTML output ----------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of built-in themes.
html_theme = "furo"
# html_theme = "pydata_sphinx_theme"
html_theme_options = {
    "github_url": "https://github.com/ingwersen-erik/peloptimize",
    "show_prev_next": False,
    "navbar_end": ["search-field.html", "navbar-icon-links.html"],
    "light_logo": "EY_logo_1.gif",
    "dark_logo": "EY_logo_1.gif",
}

html_sidebars = {"**": []}

# Napoleon settings
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = True
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = True
napoleon_use_ivar = True
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = True
napoleon_type_aliases = None
napoleon_attr_annotations = True

source_suffix = {
    '.rst': 'restructuredtext', '.txt': 'restructuredtext', '.md': 'markdown'
}


# Example configuration for intersphinx: refer to the Python standard library.
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'numpy': ('https://numpy.org/devdocs/', None),
    'sklearn': ('https://scikit-learn.org/stable/', None),
}

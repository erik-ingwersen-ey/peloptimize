"""
Nox sessions.

To use this script, you need to install `nox` and `nox-poetry` into your
development environment. You can then run the sessions using the command:

.. code-block:: bash

    $ nox

Sessions Available
------------------
- safety: scan dependencies for insecure packages.
- mypy: type-check the PACKAGE.
- tests: run the test suite.
- coverage: check code coverage quickly with the default Python.
- typeguard: check runtime type safety.
- xdoctest: run examples in docstrings.
- docs_build: invoke sphinx-build to build the HTML docs.
- docs: build the documentation.

"""

import os
import shlex
import shutil
import sys
from pathlib import Path
from textwrap import dedent

import nox


try:
    from nox_poetry import Session
    from nox_poetry import session
except ImportError:
    message = f"""\
    Nox failed to import the 'nox-poetry' PACKAGE.

    Please install it using the following command:

    {sys.executable} -m pip install nox-poetry"""
    raise SystemExit(dedent(message)) from None


PACKAGE = "pelopt"
python_versions = ["3.10", "3.9", "3.8", "3.7"]
nox.needs_version = ">= 2021.6.6"
nox.options.sessions = (
    "pre-commit",
    "safety",
    "mypy",
    "tests",
    "typeguard",
    "xdoctest",
    "docs-build",
)


def activate_virtualenv_in_recommit_hooks(
    session: Session  # pylint: disable=redefined-outer-name
) -> None:
    """Activate virtualenv in hooks installed by pre-commit.

    This function patches git hooks installed by pre-commit to activate the
    session's virtual environment. This allows pre-commit to locate hooks in
    that environment when invoked from git.

    Parameters
    ----------
    session : Session
        The Session object.
    """
    if not hasattr(session, "bin"):
        raise AttributeError(
            "`session` must be a nox-poetry Session object. "
            "No `bin` attribute found."
        )
    if session.bin is None:
        raise ValueError("`session.bin` is None. Cannot activate virtualenv.")

    # Only patch hooks containing a reference to this session's bindir. Support
    # quoting rules for Python and bash, but strip the outermost quotes, so we
    # can detect paths within the bindir, like <bindir>/python.
    bindirs = [
        bindir[1:-1] if bindir[0] in "'\"" else bindir
        for bindir in (repr(session.bin), shlex.quote(session.bin))
    ]

    virtualenv = session.env.get("VIRTUAL_ENV")
    if virtualenv is None:
        return

    headers = {
        # pre-commit < 2.16.0
        "python": f"""\
            import os
            os.environ["VIRTUAL_ENV"] = {virtualenv!r}
            os.environ["PATH"] = os.pathsep.join((
                {session.bin!r},
                os.environ.get("PATH", ""),
            ))
            """,
        # pre-commit >= 2.16.0
        "bash": f"""\
            VIRTUAL_ENV={shlex.quote(virtualenv)}
            PATH={shlex.quote(session.bin)}"{os.pathsep}$PATH"
            """,
        # pre-commit >= 2.17.0 on Windows forces sh shebang
        "/bin/sh": f"""\
            VIRTUAL_ENV={shlex.quote(virtualenv)}
            PATH={shlex.quote(session.bin)}"{os.pathsep}$PATH"
            """,
    }

    hookdir = Path(".git").joinpath("hooks")
    if not hookdir.is_dir():
        return

    for hook in hookdir.iterdir():
        if (
            hook.name.endswith(".sample")
            or not hook.is_file()
            or not hook.read_bytes().startswith(b"#!")
        ):
            continue

        text = hook.read_text()

        if not any(
            Path("A") == Path("a") and bindir.lower() in text.lower() or bindir in text
            for bindir in bindirs
        ):
            continue

        lines = text.splitlines()
        for executable, header in headers.items():
            if executable in lines[0].lower():
                lines.insert(1, dedent(header))
                hook.write_text("\n".join(lines))
                break


@session(name="pre-commit", python=python_versions[0])
def precommit(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Lint using pre-commit.

    Parameters
    ----------
    session : Session
        The Session object.
    """
    args = session.posargs or [
        "run",
        "--all-files",
        "--hook-stage=manual",
        "--show-diff-on-failure",
    ]
    session.install(
        "black",
        "darglint",
        "flake8",
        "flake8-bandit",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-rst-docstrings",
        "isort",
        "pep8-naming",
        "pre-commit",
        "pre-commit-hooks",
        "pyupgrade",
    )
    session.run("pre-commit", *args)
    if args and args[0] == "install":
        activate_virtualenv_in_recommit_hooks(session)


@session(python=python_versions[0])
def safety(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Scan dependencies for insecure packages.

    Parameters
    ----------
    session : Session
        The Session object.
    """
    requirements = session.poetry.export_requirements()
    session.install("safety")
    session.run("safety", "check", "--full-report", f"--file={requirements}")


@session(python=python_versions)
def mypy(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Type-check using mypy.

    Parameters
    ----------
    session : Session
        The Session object.
    """
    args = session.posargs or ["src", "tests", "docs/conf.py"]
    session.install(".")
    session.install("mypy", "pytest")
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions)
def tests(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Run the test suite.

    Parameters
    ----------
    session : Session
        The Session object.
    """
    session.install(".")
    session.install("coverage[toml]", "pytest", "pygments")
    try:
        session.run("coverage", "run", "--parallel", "-m", "pytest", *session.posargs)
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@session(python=python_versions[0])
def coverage(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Produce the coverage report.

    Parameters
    ----------
    session : Session
        The Session object.
    """
    args = session.posargs or ["report"]
    session.install("coverage[toml]")

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@session(python=python_versions[0])
def typeguard(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Runtime type checking using Typeguard.

    Parameters
    ----------
    session : Session
        The Session object.
    """
    session.install(".")
    session.install("pytest", "typeguard", "pygments")
    session.run("pytest", f"--typeguard-packages={PACKAGE}", *session.posargs)


@session(python=python_versions)
def xdoctest(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Run examples with xdoctest."""
    if session.posargs:
        args = [PACKAGE, *session.posargs]
    else:
        args = [f"--modname={PACKAGE}", "--command=all"]
        if "FORCE_COLOR" in os.environ:
            args.append("--colored=1")

    session.install(".")
    session.install("xdoctest[colors]")
    session.run("python", "-m", "xdoctest", *args)


@session(name="docs-build", python=python_versions[0])
def docs_build(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Build the documentation."""
    args = session.posargs or ["docs", "docs/_build"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    session.install(".")
    session.install("sphinx", "sphinx-click", "furo", "myst-parser")

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)


@session(python=python_versions[0])
def docs(session: Session) -> None:  # pylint: disable=redefined-outer-name
    """Build and serve the documentation with live reloading on file changes."""
    args = session.posargs or ["--open-browser", "docs", "docs/_build"]
    session.install(".")
    session.install("sphinx", "sphinx-autobuild", "sphinx-click",
                    "furo", "myst-parser")

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)
    session.run("sphinx-autobuild", *args)

"""Noxfile."""

import os

import nox

PACKAGE_DIR = "wingline"
TESTS_DIR = "tests"

nox.options.envdir = os.environ.get("NOX_CACHE")
nox.options.sessions = [
    "test",
    "coverage",
    "lint_flake8",
    "lint_precommit",
    "lint_pylint",
    "lint_mypy",
]


@nox.session(python=["3.10", "3.9", "3.8", "3.7"])
def test(session):
    """Run tests."""

    tests = session.posargs or [TESTS_DIR]
    session.install("-v", ".[tests]", silent=True)
    session.run("python", "-m", "coverage", "erase")
    session.run(
        "python",
        "-m",
        "pytest",
        "--numprocesses=auto",
        "--cov",
        PACKAGE_DIR,
        "--cov-append",
        "--cov-report=",
        *tests,
    )


@nox.session(python=["3.10"])
def coverage(session):
    """Check coverage."""

    session.install("-v", ".[tests]", silent=True)
    session.run(
        "python",
        "-m",
        "coverage",
        "report",
        "--show-missing",
        "--skip-covered",
        "--sort=miss",
        "--fail-under",
        "100",
    )


@nox.session(python=["3.10"], reuse_venv=True)
def lint_flake8(session):
    """Lint with flake8."""

    files = session.posargs or [PACKAGE_DIR]
    session.install("flake8")
    session.run(
        "python",
        "-m",
        "flake8",
        *files,
    )


@nox.session(python=["3.10"], reuse_venv=True)
def lint_precommit(session):
    """Lint with precommit."""

    session.install("pre-commit")
    session.run(
        "python",
        "-m",
        "pre_commit",
        "run",
        "--all-files",
    )


@nox.session(python=["3.10"], reuse_venv=True)
def lint_mypy(session):
    """Check types with mypy."""

    files = session.posargs or [PACKAGE_DIR]
    session.install(".[tests,lint]")
    session.run(
        "python",
        "-m",
        "mypy",
        *files,
    )


@nox.session(python=["3.10"], reuse_venv=True)
def lint_pylint(session):
    """Lint with pylint."""

    files = session.posargs or [PACKAGE_DIR]
    session.install(".[tests,lint]")
    session.run(
        "python",
        "-m",
        "pylint",
        *files,
    )


@nox.session(python=["3.10"])
def security_bandit(session):
    """Check for security issues in dependencies."""

    # Include all extras here to check all is safe for ci.
    session.install(".[dev,lint,tests,security]")

    session.run("python", "-m", "bandit", "-r", PACKAGE_DIR)


@nox.session(python=["3.10"])
def security_safety(session):
    """Check for security issues in dependencies."""

    # Include all extras here to check all is safe for ci.
    session.install(".[dev,lint,tests,security]")

    session.run("python", "-m", "safety", "check")


@nox.session(python=["3.10"], reuse_venv=True)
def dev(session):
    """Run tests quickly for dev work."""

    session.install("poetry")
    session.run_always(
        "poetry", "install", "-Edev", "-Etests", "-Elint", "-Esecurity", external=True
    )
    tests = session.posargs or [TESTS_DIR]
    session.run("python", "-m", "coverage", "erase")
    session.run(
        "python",
        "-m",
        "pytest",
        "--failed-first",
        "--pdbcls",
        "pudb.debugger:Debugger",
        "--pdb",
        "--capture=no",
        "--exitfirst",
        "-n",
        "auto",
        "--log-cli-level=DEBUG",
        f"--cov={PACKAGE_DIR}",
        "--cov-append",
        "--cov-report=",
        *tests,
    )
    session.run(
        "python",
        "-m",
        "coverage",
        "report",
        "--show-missing",
        "--skip-covered",
        "--sort=miss",
        "--fail-under",
        "100",
    )
    session.run("python", "-m", "bandit", "-r", PACKAGE_DIR)
    session.run("python", "-m", "safety", "check")
    session.run(
        "python",
        "-m",
        "flake8",
        PACKAGE_DIR,
        *tests,
    )
    session.run(
        "python",
        "-m",
        "pylint",
        PACKAGE_DIR,
    )
    session.run(
        "python",
        "-m",
        "mypy",
        PACKAGE_DIR,
        silent=False,
    )

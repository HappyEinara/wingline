"""Dev/ci tasks."""

import sys
import warnings

import git
import git.repo
import invoke
import semver

import wingline as package

RELEASE_BRANCH = "main"
REMOTE = "origin"
TAG_COMMAND = "make tag-release"


class VersionError(RuntimeError):
    """Raised when there's an issue with the version."""


class GitError(VersionError):
    """Raised when there's an issue getting the last release version from git."""


class NoReleaseTagError(GitError):
    """Raised when there is no release tag in the history."""


def _validate_version(version: str) -> None:
    """Check the version is valid."""

    if not version:
        raise VersionError("No version found.")
    if not (
        semver.VersionInfo.isvalid(version)
        and semver.VersionInfo.parse(version).compare("0.0.0") > 0
    ):
        raise VersionError(f"Version {version} is invalid.")


def _get_last_release() -> str:
    """Get the last release from the Git history."""

    repo = _get_repo()
    git_description = repo.git.describe(always=True, tags=True)
    # Split on first hyphen if present and strip any leading 'v'
    released_version = git_description.split("-")[0].strip().lstrip("v")
    return released_version


def _get_current_version() -> str:
    """Get the current version."""

    version = package.__version__
    return version


def _check_version() -> str:
    """Check all aspects of the current version."""
    version = _get_current_version()
    _validate_version(version)
    released_version = _get_last_release()
    try:
        _validate_version(released_version)
    except VersionError:
        raise NoReleaseTagError(
            "Couldn't find a valid release tag "
            f"(git description is '{released_version}'). "
            f"If this is a new project, try `{TAG_COMMAND}` "
            f"on a clean commit on {RELEASE_BRANCH}"
        )

    if (
        not semver.VersionInfo.parse(version).compare(
            semver.VersionInfo.parse(released_version)
        )
        > 0
    ):
        raise VersionError(
            f"Current version '{version}' is not higher than the last tagged release "
            f"'{released_version}'"
        )

    return version


def _get_repo() -> git.repo.Repo:
    """Get the current git repo."""

    try:
        repo = git.repo.Repo()
    except git.exc.InvalidGitRepositoryError:
        raise GitError("Not in a Git repo.")
    return repo


def _validate_branch(branch: str, remote_name: str) -> None:
    """Get the current branch."""

    repo = _get_repo()
    active_branch = repo.active_branch
    if active_branch != repo.refs[branch]:
        raise GitError(f"Active branch is '{active_branch}' (should be '{branch}')")
    if repo.is_dirty():
        raise GitError(f"Active branch '{active_branch}' is dirty.")
    remote = repo.remotes[remote_name]
    remote.fetch()

    unpulled = len(list(repo.iter_commits(f"{active_branch}..{active_branch}@{{u}}")))
    unpushed = len(list(repo.iter_commits(f"{active_branch}@{{u}}..{active_branch}")))

    if unpushed + unpulled:
        raise GitError(
            f"Current branch {active_branch} is not synced with remote {remote_name} "
            f"(⇡{unpushed} ⇣{unpulled})"
        )


@invoke.task
def check_version(context):
    """Check the version is present, valid and ahead of the last release."""

    try:
        _check_version()
    except NoReleaseTagError as exc:
        print(exc)
        sys.exit(1)


@invoke.task
def tag_release(context):
    """Tag the current commit on main with the latest version."""

    version = _get_current_version()
    _validate_version(version)
    try:
        version = _check_version()
    except NoReleaseTagError:
        warnings.warn("No release tag was found. Creating the first one.")
        pass
    _validate_branch(RELEASE_BRANCH, REMOTE)
    tag_label = f"v{version}"
    tag_message = f"Release {tag_label}"
    repo = _get_repo()
    repo.create_tag(tag_label, message=tag_message)
    print(f"Tagged with '{tag_label}' ('{tag_message}')")
    print(f"Run `git push {REMOTE} {tag_label}` to update the remote.")

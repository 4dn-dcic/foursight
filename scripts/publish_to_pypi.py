# Script to publish the Python package in the CURRENT git repo to PyPi.
# Does the following checks before allowing a publish:
#
# 1. The git repo MUST NOT contain unstaged changes.
# 2. The git repo MUST NOT contain staged but uncommitted changes.
# 3. The git repo MUST NOT contain committed but unpushed changes.
# 4. The git repo package directories MUST NOT contain untracked files,
#    OR if they do contain untracked files then you must confirm this is OK.
# 5. The version being published must NOT have already been published. 
#
# ASSUMES you have these environment variables correctly set for PyPi publishing:
#
# - PYPI_USER
# - PYPI_PASSWORD
#
# Prompts for yes or no before publish is actually done. There is a --noconfirm
# option to skip this confimation, however it is only allowed when running in the
# context of GitHub actions - it checks for the GITHUB_ACTIONS environment variable.
#
# FYI: This was created late April 2023 after a junk file containing development
# logging output containing passwords was accidentally published to PyPi;
# item #4 above specifically addresses/prevents this. Perhaps better
# would be if publishing only happened via GitHub actions.

import argparse
import os
import requests
import subprocess
import toml
from typing import Tuple, Union


DEBUG = False


def main() -> None:

    def is_github_actions_context():
        return "GITHUB_ACTIONS" in os.environ

    argp = argparse.ArgumentParser()
    argp.add_argument("--noconfirm", required=False, dest="noconfirm", action="store_true")
    argp.add_argument("--debug", required=False, dest="debug", action="store_true")
    args = argp.parse_args()

    if args.debug:
        global DEBUG
        DEBUG = True

    if args.noconfirm and not is_github_actions_context():
        print("The --noconfirm flag is only allowed within GitHub actions!")
        exit_with_no_action()

    if not verify_unstaged_changes():
        exit_with_no_action()

    if not verify_uncommitted_changes():
        exit_with_no_action()

    if not verify_unpushed_changes():
        exit_with_no_action()

    if not verify_tagged():
        exit_with_no_action()

    if not verify_untracked_files():
        exit_with_no_action()

    package_name = get_package_name()
    package_version = get_package_version()

    if not verify_not_already_published(package_name, package_version):
        exit_with_no_action()

    if not args.noconfirm:
        if not answered_yes_to_confirmation(f"Do you want to publish {package_name} {package_version} to PyPi?"):
            exit_with_no_action()

    print(f"Publishing {package_name} {package_version} to PyPi ...")

    if not publish_package():
        exit_with_no_action()

    print(f"Publishing {package_name} {package_version} to PyPi complete.")


def publish_package(pypi_username: str = None, pypi_password: str = None) -> bool:
    if not pypi_username:
        pypi_username = os.environ.get("PYPI_USER")
    if not pypi_password:
        pypi_password = os.environ.get("PYPI_PASSWORD")
    if not pypi_username or not pypi_password:
        print(f"No PyPi credentials. You must have PYPI_USER and PYPI_PASSWORD environment variables set.")
        return False
    poetry_publish_command = [
        "poetry", "publish",
        "--no-interaction", "--build",
        f"--username={pypi_username}", f"--password={pypi_password}"
    ]
    poetry_publish_results, status_code = execute_command(poetry_publish_command)
    print("\n".join(poetry_publish_results))
    if status_code != 0:
        print(f"Publish to PyPi failed!")
        return False
    return True


def verify_unstaged_changes() -> bool:
    """
    If the current git repo has no unstaged changes then returns True,
    otherwise prints a warning and returns False.
    """
    git_diff_results, _ = execute_command(["git", "diff"])
    if git_diff_results:
        print("You have changes to this branch that you have not staged for commit.")
        return False
    return True


def verify_uncommitted_changes() -> bool:
    """
    If the current git repo has no staged but uncommitted changes then returns True,
    otherwise prints a warning and returns False.
    """
    git_diff_staged_results, _ = execute_command(["git", "diff", "--staged"])
    if git_diff_staged_results:
        print("You have staged changes to this branch that you have not committed.")
        return False
    return True


def verify_unpushed_changes() -> bool:
    """
    If the current git repo committed but unpushed changes then returns True,
    otherwise prints a warning and returns False.
    """
    git_uno_results, _ = execute_command(["git", "status", "-uno"], lines_containing="is ahead of")
    if git_uno_results:
        print("You have committed changes to this branch that you have not pushed.")
        return False
    return True


def verify_tagged() -> bool:
    """
    If the current git repo has a tag as its most recent commit then returns True,
    otherwise prints a warning and returns False.
    """
    git_most_recent_commit, _ = execute_command(["git", "log", "-1", "--decorate"], lines_containing="tag:")
    if not git_most_recent_commit:
        print("You can only publish a tagged commit.")
        return False
    return True


def verify_untracked_files() -> bool:
    """
    If the current git repo has no untracked files then returns True,
    otherwise prints a warning, and with the list of untraced files,
    and prompts the user for a yes/no confirmation on whether or to
    continue, and returns True for a yes response, otherwise returns False.
    """
    untracked_files = get_untracked_files()
    if untracked_files:
        print(f"WARNING: You are about to PUBLISH the following ({len(untracked_files)})"
              f" UNTRACKED file{'' if len(untracked_files) == 1 else 's' } -> SECURITY risk:")
        for untracked_file in untracked_files:
            print(f"-- {untracked_file}")
        print("DO NOT continue UNLESS you KNOW what you are doing!")
        if not answered_yes_to_confirmation("Do you really want to continue?"):
            return False
    return True


def verify_not_already_published(package_name: str, package_version: str) -> bool:
    """
    If the given package and version has not already been published to PyPi then returns True,
    otherwise prints a warning and returns False.
    """
    response = requests.get(f"https://pypi.org/project/{package_name}/{package_version}/")
    if response.status_code == 200:
        print(f"Package {package_name} {package_version} has already been published to PyPi.")
        return False
    return True


def get_untracked_files() -> list:
    """
    Returns a list of untracked files for the current git repo; empty list of no untracked changes.
    """
    package_directories = get_package_directories()
    untracked_files = []
    for package_directory in package_directories:
        git_status_results, _ = execute_command(["git", "status", "-s", package_directory])
        for git_status_result in git_status_results:
            if git_status_result and git_status_result.startswith("??"):
                untracked_file = git_status_result[2:].strip()
                if untracked_file:
                    untracked_files.append(untracked_file)
    return untracked_files


def get_package_version() -> str:
    """
    Returns the tag name of the most recently created tag in the current git repo.
    """
    tag_commit, _ = execute_command("git rev-list --tags --max-count=1")
    tag_name, _ = execute_command(f"git  describe --tags  {tag_commit[0]}")
    package_version = tag_name[0]
    if package_version.startswith("v"):
        package_version = package_version[1:]
    return package_version


def get_package_name() -> str:
    """
    Returns the base name of the current git repo name.
    """
    package_name, _ = execute_command("git config --get remote.origin.url".split(" "))
    package_name = os.path.basename(package_name[0])
    if package_name.endswith(".git"):
        package_name = package_name[:-4]
    return package_name


def get_package_directories() -> list:
    """
    Returns a list of directories constituting the Python package of the current repo,
    according to the pyproject.toml file.
    """
    package_directories = []
    with open("pyproject.toml", "r") as f:
        pyproject_toml = toml.load(f)
    pyproject_package_directories = pyproject_toml["tool"]["poetry"]["packages"]
    for pyproject_package_directory in pyproject_package_directories:
        package_directory = pyproject_package_directory.get("include")
        if package_directory:
            package_directory_from = pyproject_package_directory.get("from")
            if package_directory_from:
                package_directory = os.path.join(package_directory_from, package_directory)
            package_directories.append(package_directory)
    return package_directories


def execute_command(command_argv: Union[list, str], lines_containing: str = None) -> Tuple[list, int]:
    """
    Executes the given command as a command-line subprocess, and returns a tuple whose first element
    is the list of lines from the output of the command, and the second element is the status code.
    """
    def cleanup_funny_output(output: str) -> str:
        return output.replace("('", "").replace("',)", "").replace("\\n\\n", "\n").replace("\\n", "\n")

    if isinstance(command_argv, str):
        command_argv = [arg for arg in command_argv.split(" ") if arg.strip()]
    if DEBUG:
        print(f"DEBUG: {' '.join(command_argv)}")
    result = subprocess.run(command_argv, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    lines = result.stdout.decode("utf-8").split("\n")
    if lines_containing:
        lines = [line for line in lines if lines_containing in line]
    return [cleanup_funny_output(line.strip()) for line in lines if line.strip()], result.returncode


def answered_yes_to_confirmation(message: str) -> bool:
    """
    Prompts the user with the given message and asks for a yes or no answer,
    and if yes is the user response then returns True, otherwise returns False.
    """
    response = input(f"{message} [yes | no]: ").lower()
    if response == "yes":
        return True
    return False


def exit_with_no_action() -> None:
    """
    Exits this process immediately with status 1;
    first prints a message saying no action was taken.
    """
    print("Exiting without taking action.")
    exit(1)


if __name__ == "__main__":
    main()

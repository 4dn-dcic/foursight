from foursight_core.scripts.local_check_runner import local_check_runner
from chalicelib_fourfront.app_utils import app_utils_obj as app_utils


def main():
    local_check_runner(app_utils)


if __name__ == "__main__":
    main()

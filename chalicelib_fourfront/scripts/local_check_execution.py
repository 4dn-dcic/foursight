from foursight_core.captured_output import captured_output
from foursight_core.scripts.local_check_execution import local_check_execution
with captured_output():
    from chalicelib_fourfront.app_utils import app_utils_obj as app_utils


def main():
    local_check_execution(app_utils)


if __name__ == "__main__":
    main()

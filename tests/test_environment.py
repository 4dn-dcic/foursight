from conftest import *


class TestEnvironment():
    environ = DEV_ENV

    def test_list_environments(self):
        env_obj = environment.Environment(FOURSIGHT_PREFIX)
        env_list = env_obj.list_environment_names()
        # assume we have at least one environments
        assert (isinstance(env_list, list))
        assert (self.environ in env_list)


from conftest import *


class TestActionResult():
    act_name = 'test_only_action'
    environ = DEV_ENV  # hopefully this is up
    app_utils_obj = app_utils.AppUtils()
    connection = app_utils_obj.init_connection(environ)

    def test_action_result_methods(self):
        action = run_result.ActionResult(self.connection, self.act_name)
        res = action.store_result()
        assert (res.get('status') == 'PEND')
        assert (res.get('output') is None)
        assert (res.get('type') == 'action')
        assert ('uuid' in res.get('kwargs'))
        action.kwargs = {'do_not_store': True}
        unstored_res = action.store_result() # will not update latest result
        assert ('do_not_store' in unstored_res['kwargs'])
        res2 = action.get_latest_result()
        assert (res == res2)
        # bad status
        action.kwargs = {'abc': 123}
        action.status = 'NOT_VALID'
        res = action.store_result()
        assert (res.get('status') == 'FAIL')
        assert (res.get('description') == 'Malformed status; look at Foursight action definition.')
        assert (res['kwargs']['abc'] == 123)
        assert ('uuid' in res.get('kwargs'))
        # this action has no check_name/called_by kwargs, so expect KeyError
        with pytest.raises(KeyError) as exc:
            action.get_associated_check_result(action.kwargs)

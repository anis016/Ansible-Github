import mock
import library.time_now as time_now
from nose.tools import assert_equals

@mock.patch('library.show_time.check_if_route_exists')
@mock.patch('library.show_time.AnsibleModule')
def test_show_time(mock_module,
                   mock_show_time):
    """
    prefix_check - test module arguments
    """
    time_now.main()
    mock_module.assert_called_with(
        argument_spec={
          'prefix': {'required': True, 'type': 'str'},
          'timeout': {'type': 'int', 'default': 5},
        })
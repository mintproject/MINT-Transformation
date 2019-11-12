import pytest
from dtran.config_parser import ConfigParser
from unittest.mock import patch, call, MagicMock
from funcs import ReadFunc, UnitTransFunc, GraphWriteFunc


@pytest.mark.parametrize("config_path", [
    "test/sample_config.json", "test/sample_config.yml"
])
@patch('dtran.config_parser.Pipeline')
def test_config_parser_no_user_inputs(pipeline_mock, config_path):
    pipeline_mock.return_value = MagicMock()

    parser = ConfigParser({})
    parsed_pipeline, parsed_inputs = parser.parse(config_path)
    assert "$/liter" in parsed_inputs.values()
    assert pipeline_mock.mock_calls[-1] == call(
        [ReadFunc, UnitTransFunc, GraphWriteFunc],
        [(['unit_trans', 1, 'graph'], ['read_func', 1, 'data']), (['graph_write_func', 1, 'graph'], ['unit_trans', 1, 'graph'])]
    )


@pytest.mark.parametrize("config_path", [
    "test/sample_config.json", "test/sample_config.yml"
])
@patch('dtran.config_parser.Pipeline')
def test_config_parser_with_user_inputs(pipeline_mock, config_path):
    pipeline_mock.return_value = MagicMock()

    parser = ConfigParser({("MyCustomName2", "unit_desired"): "$/oz"})
    parsed_pipeline, parsed_inputs = parser.parse(config_path)
    assert "$/oz" in parsed_inputs.values()
    assert pipeline_mock.mock_calls[-1] == call(
        [ReadFunc, UnitTransFunc, GraphWriteFunc],
        [(['unit_trans', 1, 'graph'], ['read_func', 1, 'data']),
         (['graph_write_func', 1, 'graph'], ['unit_trans', 1, 'graph'])]
    )

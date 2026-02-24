from pyfdb._internal import UserInputMapper


def test_single_value_internal():
    mars_selection = UserInputMapper.map_selection_to_internal({"key-1": "value-1"})

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-1"]


def test_single_value_external():
    mars_selection = UserInputMapper.map_selection_to_external({"key-1": "value-1"})

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-1"


def test_multi_value_internal():
    mars_selection = UserInputMapper.map_selection_to_internal(
        {
            "key-1": ["value-1", "value-2", "value-3"],
            "key-2": ["value-2"],
            "key-3": ["value-3", 214, 213.54],
            "key-4": [120, 123, 124, 125],
        }
    )

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-1", "value-2", "value-3"]
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == ["value-2"]
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == ["value-3", "214", "213.54"]
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == ["120", "123", "124", "125"]


def test_multi_value_external():
    mars_selection = UserInputMapper.map_selection_to_external(
        {
            "key-1": ["value-1", "value-2", "value-3"],
            "key-2": ["value-2"],
            "key-3": ["value-3", "214", "213.54"],
            "key-4": ["120", "123", "124", "125"],
        }
    )

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-1", "value-2", "value-3"]
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == ["value-2"]
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == ["value-3", "214", "213.54"]
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == ["120", "123", "124", "125"]


def test_to_internal():
    mars_selection = UserInputMapper.map_selection_to_internal(
        {
            "key-1": ["value-2", "value-4"],
            "key-2": ["0.1", 0.2],  # Mixed
            "key-3": [0.1, 0.2],  # Floating
            "key-4": [1, 2],  # Integer
        }
    )

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-2", "value-4"]
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == ["0.1", "0.2"]
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == ["0.1", "0.2"]
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == ["1", "2"]


def test_to_external():
    mars_selection = UserInputMapper.map_selection_to_external(
        {
            "key-1": ["value-2", "value-4"],
            "key-2": ["0.1", "0.2"],  # Mixed
            "key-3": ["0.1", "0.2"],  # Floating
            "key-4": ["1", "2"],  # Integer
        }
    )

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-2", "value-4"]
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == ["0.1", "0.2"]
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == ["0.1", "0.2"]
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == ["1", "2"]


def test_overwrite_key_internal():
    mars_selection = UserInputMapper.map_selection_to_internal(
        {
            "key-1": "value-1",
            "key-1": ["value-3", "214", "213.54"],
        }
    )

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-3", "214", "213.54"]


def test_overwrite_key_external():
    mars_selection = UserInputMapper.map_selection_to_external(
        {
            "key-1": "value-1",
            "key-1": ["value-3", "214", "213.54"],
        }
    )

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-3", "214", "213.54"]


def test_pythonic_interface():
    mars_selection = UserInputMapper.map_selection_to_internal(
        {
            "key-1": ["value-1", "value-2", "value-3"],
            "key-2": 0.1,
            "key-3": [x for x in range(1, 5)],
            "key-4": [0.1, "0.2"],
            "key-5": [1 + 0.5 * x for x in range(2)],
        }
    )

    assert len(mars_selection) == 5
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == ["value-1", "value-2", "value-3"]
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == ["0.1"]
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == ["1", "2", "3", "4"]
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == ["0.1", "0.2"]
    assert "key-5" in mars_selection
    assert mars_selection["key-5"] == ["1.0", "1.5"]

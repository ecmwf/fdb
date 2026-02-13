from pyfdb.pyfdb_type import MarsSelectionMapper


def test_single_value():
    mars_selection = MarsSelectionMapper.map({"key-1": "value-1"})

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-1"


def test_multi_value():
    mars_selection = MarsSelectionMapper.map(
        {
            "key-1": ["value-1", "value-2", "value-3"],
            "key-2": ["value-2", "value-4"],
            "key-3": ["value-3", 214, 213.54],
            "key-4": [120, 123, 124, 125],
        }
    )

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-1/value-2/value-3"
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == "value-2/value-4"
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == "value-3/214/213.54"
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == "120/123/124/125"


def test_to():
    mars_selection = MarsSelectionMapper.map(
        {
            "key-1": ["value-2", "value-4"],
            "key-2": ["0.1", 0.2],  # Mixed
            "key-3": [0.1, 0.2],  # Floating
            "key-4": [1, 2],  # Integer
        }
    )

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-2/value-4"
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == "0.1/0.2"
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == "0.1/0.2"
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == "1/2"


def test_to_by():
    mars_selection = MarsSelectionMapper.map(
        {
            "key-1": ["value-2", "value-4", "value-5"],
            "key-1.1": ["value-2/to/value-4/by/value-5"],
            "key-1.2": ["value-2", "to", "value-4", "by", "value-5"],
            "key-2": ["0.1", 0.2, "0.5"],  # Mixed
            "key-2.1": ["0.1", 0.2, 0.5],  # Mixed
            "key-2.2": ["0.1", "to", 0.2, "by", 0.5],  # Mixed
            "key-3": [0.1, 0.2, 0.05],  # Floating
            "key-3.1": [0.1, "to", 0.2, "by", 0.05],  # Floating
            "key-4": [1, 2, 0.5],  # Integer
            "key-4.1": [1, "to", 2, "by", 0.5],  # Integer
        }
    )

    assert len(mars_selection) == 10
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-2/value-4/value-5"
    assert "key-1.1" in mars_selection
    assert mars_selection["key-1.1"] == "value-2/to/value-4/by/value-5"
    assert "key-1.2" in mars_selection
    assert mars_selection["key-1.2"] == "value-2/to/value-4/by/value-5"
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == "0.1/0.2/0.5"
    assert "key-2.1" in mars_selection
    assert mars_selection["key-2.1"] == "0.1/0.2/0.5"
    assert "key-2.2" in mars_selection
    assert mars_selection["key-2.2"] == "0.1/to/0.2/by/0.5"
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == "0.1/0.2/0.05"
    assert "key-3.1" in mars_selection
    assert mars_selection["key-3.1"] == "0.1/to/0.2/by/0.05"
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == "1/2/0.5"
    assert "key-4.1" in mars_selection
    assert mars_selection["key-4.1"] == "1/to/2/by/0.5"


def test_mixed_functionality():
    mars_selection = MarsSelectionMapper.map(
        {
            "key-1": "value-1",
            "key-2": ["value-3", 214, 213.54],
            "key-3": ["0.1", "to", 0.2],
            "key-4": ["0.1", "to", 0.2, "by", "0.5"],
        }
    )

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-1"
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == "value-3/214/213.54"
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == "0.1/to/0.2"
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == "0.1/to/0.2/by/0.5"


def test_overwrite_key():
    mars_selection = MarsSelectionMapper.map(
        {
            "key-1": "value-1",
            "key-1": ["value-3", 214, 213.54],
        }
    )

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-3/214/213.54"


def test_pythonic_interface():
    mars_selection = MarsSelectionMapper.map(
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
    assert mars_selection["key-1"] == "value-1/value-2/value-3"
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == "0.1"
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == "1/2/3/4"
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == "0.1/0.2"
    assert "key-5" in mars_selection
    assert mars_selection["key-5"] == "1.0/1.5"

import pytest
from pyfdb.pyfdb_type import SelectionBuilder


def test_single_value():
    builder = SelectionBuilder()

    builder.value("key-1", "value-1")

    mars_selection = builder.build()

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-1"


def test_multi_value():
    builder = SelectionBuilder()

    builder.values("key-1", ["value-1", "value-2", "value-3"])
    builder.values("key-2", ["value-2", "value-4"])
    builder.values("key-3", ["value-3", 214, 213.54])
    builder.values("key-4", [120, 123, 124, 125])

    mars_selection = builder.build()

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
    builder = SelectionBuilder()

    builder.to("key-1", "value-2", "value-4")
    builder.to("key-2", "0.1", 0.2)  # Mixed
    builder.to("key-3", 0.1, 0.2)  # Floating
    builder.to("key-4", 1, 2)  # Integer

    mars_selection = builder.build()

    assert len(mars_selection) == 4
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-2/to/value-4"
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == "0.1/to/0.2"
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == "0.1/to/0.2"
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == "1/to/2"


def test_to_by():
    builder = SelectionBuilder()

    builder.to_by("key-1", "value-2", "value-4", "value-5")
    builder.to_by("key-2", "0.1", 0.2, "0.5")  # Mixed
    builder.to_by("key-2.1", "0.1", 0.2, 0.5)  # Mixed
    builder.to_by("key-3", 0.1, 0.2, 0.05)  # Floating
    builder.to_by("key-4", 1, 2, 0.5)  # Integer

    mars_selection = builder.build()

    assert len(mars_selection) == 5
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-2/to/value-4/by/value-5"
    assert "key-2" in mars_selection
    assert mars_selection["key-2"] == "0.1/to/0.2/by/0.5"
    assert "key-2.1" in mars_selection
    assert mars_selection["key-2.1"] == "0.1/to/0.2/by/0.5"
    assert "key-3" in mars_selection
    assert mars_selection["key-3"] == "0.1/to/0.2/by/0.05"
    assert "key-4" in mars_selection
    assert mars_selection["key-4"] == "1/to/2/by/0.5"


def test_mixed_functionality():
    builder = SelectionBuilder()

    mars_selection = (
        builder.value("key-1", "value-1")
        .values("key-2", ["value-3", 214, 213.54])
        .to("key-3", "0.1", 0.2)
        .to_by("key-4", "0.1", 0.2, "0.5")
        .build()
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
    builder = SelectionBuilder()

    mars_selection = (
        builder.value("key-1", "value-1")
        .values("key-1", ["value-3", 214, 213.54])
        .build()
    )

    assert len(mars_selection) == 1
    assert "key-1" in mars_selection
    assert mars_selection["key-1"] == "value-3/214/213.54"


def test_overwrite_key_strict_mode():
    builder = SelectionBuilder(strict_mode=True)

    with pytest.raises(ValueError):
        _ = (
            builder.value("key-1", "value-1")
            .values("key-1", ["value-3", 214, 213.54])
            .build()
        )

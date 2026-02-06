import pytest
from pyfdb._internal import MarsRequest


def test_mars_request_verb():
    request = MarsRequest("verb")
    assert request.verb() == "verb"


def test_mars_request():
    request = MarsRequest()
    assert request is not None


def test_mars_request_default():
    mars_request = MarsRequest()

    assert mars_request.verb() == "retrieve"


def test_mars_request_no_verb_but_key_values():
    key_values = {"key1": "value1", "key2": "value3"}

    with pytest.raises(RuntimeError):
        MarsRequest(key_values=key_values)


def test_mars_request_key_values_unknown_type():
    key_values = {
        "key1": "value1",
        "key2": "value3",
        "key3": {"sub-key-1": "sub-value-1"},
    }

    with pytest.raises(ValueError):
        print(MarsRequest("retrieve", key_values=key_values))


def test_mars_request_verb_keys():
    key_values = {"key1": "value1", "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert request.verb() == "verb"
    assert request["key1"] == "value1"
    assert request["key2"] == "value3"


def test_mars_request_verb_items():
    key_values = {"key1": "value1", "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert request.verb() == "verb"
    key_values = request.items()
    assert request["key1"] == "value1"
    assert request["key2"] == "value3"


def test_mars_request_verb_key_values_flat():
    key_values = {"key1": ["value1", "value2"], "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert request.verb() == "verb"
    assert request["key1"] == ["value1", "value2"]
    assert request["key2"] == "value3"


def test_mars_request_empty():
    key_values = {"key1": ["value1", "value2"], "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert request.empty() is False
    assert MarsRequest().empty() is True


def test_mars_request_length():
    key_values = {"key1": ["value1", "value2"], "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert len(request) == 2
    assert request["key1"] == ["value1", "value2"]
    assert request["key2"] == "value3"

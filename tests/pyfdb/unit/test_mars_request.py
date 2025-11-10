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


def test_mars_request_verb_key_values():
    key_values = {"key1": "value1", "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert request.verb() == "verb"
    assert request.key_values()["key1"] == "value1"
    assert request.key_values()["key2"] == "value3"


def test_mars_request_verb_key_values_flat():
    key_values = {"key1": ["value1", "value2"], "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert request.verb() == "verb"
    assert request.key_values()["key1"] == "value1/value2"
    assert request.key_values()["key2"] == "value3"


def test_mars_request_empty():
    key_values = {"key1": ["value1", "value2"], "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert request.empty() is False
    assert MarsRequest().empty() is True


def test_mars_request_length():
    key_values = {"key1": ["value1", "value2"], "key2": "value3"}

    request = MarsRequest("verb", key_values=key_values)

    assert len(request) == 2
    print(request)

from pathlib import Path

from ._scrub import before_record_request, before_record_response


def convert_cassette(path: Path) -> bool:
    """Rewrite a snowflake-vcrpy cassette in place: gunzip and scrub bodies.

    Returns False and leaves the file alone when it is not a cassette.
    """
    from vcr.serialize import deserialize, serialize
    from vcr.serializers import yamlserializer

    try:
        requests, responses = deserialize(path.read_text(), yamlserializer)
    except KeyError:
        return False
    kept_requests, kept_responses = [], []
    for request, response in zip(requests, responses):
        scrubbed = before_record_request(request)
        if scrubbed is not None:
            kept_requests.append(scrubbed)
            kept_responses.append(before_record_response(response))
    path.write_text(
        serialize(
            {"requests": kept_requests, "responses": kept_responses}, yamlserializer
        )
    )
    return True


def convert_dir(directory: Path) -> int:
    return sum(convert_cassette(path) for path in sorted(directory.rglob("*.yaml")))

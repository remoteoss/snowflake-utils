from pathlib import Path

from ._scrub import before_record_request, before_record_response


def convert_cassette(path: Path) -> None:
    """Rewrite a snowflake-vcrpy cassette in place: gunzip and scrub bodies."""
    from vcr.serialize import deserialize, serialize
    from vcr.serializers import yamlserializer

    requests, responses = deserialize(path.read_text(), yamlserializer)
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


def convert_dir(directory: Path) -> int:
    paths = sorted(directory.rglob("*.yaml"))
    for path in paths:
        convert_cassette(path)
    return len(paths)

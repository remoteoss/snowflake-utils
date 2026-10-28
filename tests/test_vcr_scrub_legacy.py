import gzip
import json

from vcr.request import Request

from snowflake_utils.testing.vcr import _scrub


def test_gzipped_response_body_is_decoded_and_scrubbed():
    body = {"data": {"qrmk": "k" * 44, "queryId": "keep"}}
    response = {
        "headers": {
            "Content-Encoding": ["gzip"],
            "Content-Length": ["99"],
            "Date": ["x"],
        },
        "body": {"string": gzip.compress(json.dumps(body).encode())},
    }
    scrubbed = _scrub.before_record_response(response)
    payload = json.loads(scrubbed["body"]["string"])
    assert payload["data"] == {"qrmk": _scrub.SCRUBBED, "queryId": "keep"}
    assert list(scrubbed["headers"]) == ["Date"]


def test_v4_signed_params_are_dropped_from_request_uris():
    uri = (
        "https://b.s3.amazonaws.com/p/results/id/main/data_0?"
        "X-Amz-Credential=cred&X-Amz-Signature=sig&keep=1"
    )
    request = _scrub.before_record_request(Request("GET", uri, None, {}))
    assert request.uri.endswith("?keep=1")


def test_account_hosts_in_response_data_are_canonicalized():
    body = {"data": {"rowset": [["https://acme.eu-west-1.snowflakecomputing.com/x"]]}}
    response = {"headers": {}, "body": {"string": json.dumps(body).encode()}}
    text = _scrub.before_record_response(response)["body"]["string"].decode()
    assert "acme" not in text
    assert f"https://{_scrub.VOID}.snowflakecomputing.com/x" in text


def test_scrubbing_is_idempotent_on_hosts():
    body = {"data": {"rowset": [[f"{_scrub.VOID}.snowflakecomputing.com"]]}}
    response = {"headers": {}, "body": {"string": json.dumps(body).encode()}}
    once = _scrub.before_record_response(response)["body"]["string"]
    response["body"]["string"] = once
    assert _scrub.before_record_response(response)["body"]["string"] == once

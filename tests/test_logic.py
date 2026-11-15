import math
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.logic import to_celsius, validate_raw  # noqa: E402


def test_celsius_passthrough():
    fields, reason = validate_raw(
        {"box_id": "B1", "sampled_utc": "t", "reading": 5.5, "scale": "C"}
    )
    assert reason is None
    assert fields["celsius"] == 5.5
    assert fields["scale"] == "C"


def test_fahrenheit_conversion():
    fields, reason = validate_raw(
        {"box_id": "B1", "sampled_utc": "t", "reading": 41.0, "scale": "F"}
    )
    assert reason is None
    assert math.isclose(fields["celsius"], 5.0, abs_tol=1e-12)
    assert math.isclose(to_celsius(32, "F"), 0.0)
    assert math.isclose(to_celsius(212, "F"), 100.0)


def test_missing_scale_blocks():
    fields, reason = validate_raw(
        {"box_id": "B1", "sampled_utc": "t", "reading": 39.2}
    )
    assert fields is None and reason == "missing_scale"


def test_null_and_unknown_scale_block():
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "reading": 1, "scale": None})[1] == "missing_scale"
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "reading": 1, "scale": "K"})[1].startswith("unknown_scale")
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "reading": 1, "scale": "c"})[1].startswith("unknown_scale")


def test_non_finite_and_non_numeric_reading_block():
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "reading": float("nan"), "scale": "C"})[1] == "reading_not_finite"
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "reading": float("inf"), "scale": "C"})[1] == "reading_not_finite"
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "reading": "3.2", "scale": "C"})[1] == "reading_not_number"
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "reading": True, "scale": "C"})[1] == "reading_not_number"


def test_missing_fields():
    assert validate_raw({"sampled_utc": "t", "reading": 1,
                         "scale": "C"})[1] == "missing_or_invalid_box_id"
    assert validate_raw({"box_id": "b", "reading": 1,
                         "scale": "C"})[1] == "missing_or_invalid_sampled_utc"
    assert validate_raw({"box_id": "b", "sampled_utc": "t",
                         "scale": "C"})[1] == "missing_reading"
    assert validate_raw("nope")[1] == "message_not_object"

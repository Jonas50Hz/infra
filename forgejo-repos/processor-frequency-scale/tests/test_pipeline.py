"""Tests for the frequency calculation an EE normally edits."""

from __future__ import annotations

import unittest

from wama_processor.testing import input_measurement

from processor_frequency_scale.processor import (
    FREQUENCY_HZ,
    FREQUENCY_MILLIHERTZ,
    HERTZ_TO_MILLIHERTZ,
    PROCESSOR,
    transform,
)


class ProcessorTests(unittest.TestCase):
    """Prove the named input produces the expected named output."""

    def test_scales_frequency_from_hertz_to_millihertz(self) -> None:
        frequency = input_measurement(
            PROCESSOR,
            FREQUENCY_HZ,
            50.01,
            is_valid=True,
        )

        derived = transform(frequency)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, FREQUENCY_MILLIHERTZ)
        self.assertEqual(derived.double_value, 50.01 * HERTZ_TO_MILLIHERTZ)

    def test_ignores_a_frequency_without_a_double_value(self) -> None:
        frequency = input_measurement(
            PROCESSOR,
            FREQUENCY_HZ,
            None,
        )

        self.assertIsNone(transform(frequency))
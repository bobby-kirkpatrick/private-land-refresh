"""
Unit tests for utils/qc_rules.py — apply_qc_rule()

Every QC rule is tested in isolation.  No arcpy dependency; the function
is pure Python and deterministic given the same inputs.

Rule reference
--------------
Flag  Condition                                               Action
0     qc != 1 (model agreement)                             no change
2     XGB=FALSE, GIS=TRUE, private name, acres >= threshold  keep TRUE
3     XGB=FALSE, GIS=TRUE, private name, acres <  threshold  flip to FALSE
4     no owner, overlap >= GOVT_OVERLAP_THRESHOLD             TRUE
5     no owner, overlap <  GOVT_OVERLAP_THRESHOLD             UNKNOWN
6     govt_centroid=1 AND govt_owner=1                        TRUE
7     qc=1 but no other rule fires                            label unchanged
"""
from __future__ import annotations

import pytest

from configs.settings import (
    GOVT_OVERLAP_THRESHOLD,
    QC_LARGE_PARCEL_THRESHOLD,
    NULL_OWNER_SENTINEL,
)
from utils.qc_rules import apply_qc_rule


def _rule(
    gh='TRUE', xgb='FALSE', priv_own=0, acres=0.0,
    qc=1, name='John Smith', overlap=0.0, govt_cen=0, govt_own=0,
):
    """Helper to call apply_qc_rule with keyword args and sensible defaults."""
    return apply_qc_rule(gh, xgb, priv_own, acres, qc, name, overlap, govt_cen, govt_own)


class TestNoChangeWhenAgreement:
    """qc=0 means models agreed; nothing should be touched."""

    def test_true_label_unchanged(self):
        gh, flag = _rule(gh='TRUE', qc=0)
        assert gh == 'TRUE'
        assert flag == 0

    def test_false_label_unchanged(self):
        gh, flag = _rule(gh='FALSE', qc=0)
        assert gh == 'FALSE'
        assert flag == 0


class TestRule2:
    """Flag 2: XGB=FALSE, GIS=TRUE, private name, acres >= threshold → keep TRUE."""

    def test_keeps_true_for_large_private_parcel(self):
        gh, flag = _rule(
            gh='TRUE', xgb='FALSE', priv_own=1,
            acres=QC_LARGE_PARCEL_THRESHOLD,  # exactly at boundary
        )
        assert gh == 'TRUE'
        assert flag == 2

    def test_larger_parcel_also_rule_2(self):
        gh, flag = _rule(
            gh='TRUE', xgb='FALSE', priv_own=1,
            acres=QC_LARGE_PARCEL_THRESHOLD + 100,
        )
        assert gh == 'TRUE'
        assert flag == 2


class TestRule3:
    """Flag 3: XGB=FALSE, GIS=TRUE, private name, acres < threshold → flip to FALSE."""

    def test_flips_to_false_for_small_private_parcel(self):
        gh, flag = _rule(
            gh='TRUE', xgb='FALSE', priv_own=1,
            acres=QC_LARGE_PARCEL_THRESHOLD - 0.1,
        )
        assert gh == 'FALSE'
        assert flag == 3

    def test_zero_acres_triggers_rule_3(self):
        gh, flag = _rule(gh='TRUE', xgb='FALSE', priv_own=1, acres=0.0)
        assert gh == 'FALSE'
        assert flag == 3


class TestRule4:
    """Flag 4: no owner sentinel, high overlap → TRUE."""

    def test_high_overlap_no_owner_becomes_true(self):
        gh, flag = _rule(
            gh='FALSE', name=NULL_OWNER_SENTINEL,
            overlap=GOVT_OVERLAP_THRESHOLD,
        )
        assert gh == 'TRUE'
        assert flag == 4

    def test_overlap_above_threshold_also_rule_4(self):
        gh, flag = _rule(
            gh='FALSE', name=NULL_OWNER_SENTINEL,
            overlap=GOVT_OVERLAP_THRESHOLD + 5,
        )
        assert gh == 'TRUE'
        assert flag == 4


class TestRule5:
    """Flag 5: no owner sentinel, low overlap → UNKNOWN."""

    def test_low_overlap_no_owner_becomes_unknown(self):
        gh, flag = _rule(
            gh='FALSE', name=NULL_OWNER_SENTINEL,
            overlap=GOVT_OVERLAP_THRESHOLD - 1,
        )
        assert gh == 'UNKNOWN'
        assert flag == 5

    def test_zero_overlap_no_owner_is_unknown(self):
        gh, flag = _rule(gh='FALSE', name=NULL_OWNER_SENTINEL, overlap=0.0)
        assert gh == 'UNKNOWN'
        assert flag == 5


class TestRule6:
    """Flag 6: govt centroid + govt owner → TRUE."""

    def test_govt_centroid_and_owner_becomes_true(self):
        gh, flag = _rule(gh='FALSE', govt_cen=1, govt_own=1)
        assert gh == 'TRUE'
        assert flag == 6

    def test_centroid_alone_is_not_rule_6(self):
        # Only centroid, no matching owner name → falls through to rule 7
        gh, flag = _rule(gh='FALSE', govt_cen=1, govt_own=0)
        assert flag == 7

    def test_owner_alone_is_not_rule_6(self):
        gh, flag = _rule(gh='FALSE', govt_cen=0, govt_own=1)
        assert flag == 7


class TestRule7:
    """Flag 7: disagreement (qc=1) that no other rule resolves."""

    def test_unresolved_disagreement_gets_flag_7(self):
        # Private-owner parcel with moderate overlap — no rule fires specifically
        gh, flag = _rule(
            gh='TRUE', xgb='TRUE',  # models agree on TRUE, but qc=1 somehow
            priv_own=1, acres=5.0,
            overlap=50.0, govt_cen=0, govt_own=0,
        )
        assert flag == 7
        # label is left as-is
        assert gh == 'TRUE'

    def test_flag_7_preserves_false_label(self):
        gh, flag = _rule(gh='FALSE', xgb='FALSE', govt_cen=0, govt_own=0)
        assert flag == 7
        assert gh == 'FALSE'


class TestRulePriority:
    """Rules 2 and 3 take priority over rules 4–7 when conditions overlap."""

    def test_rule2_wins_over_rule6_when_priv_own_and_large(self):
        # Both rule-2 and rule-6 conditions are met; rule 2 is checked first
        gh, flag = _rule(
            gh='TRUE', xgb='FALSE', priv_own=1,
            acres=QC_LARGE_PARCEL_THRESHOLD + 1,
            govt_cen=1, govt_own=1,
        )
        assert flag == 2

    def test_no_owner_rule4_not_triggered_when_priv_own_set(self):
        # priv_own=1 means a name was found, so NULL_OWNER_SENTINEL should not
        # appear in full_name — this tests defensive behaviour
        gh, flag = _rule(
            gh='TRUE', xgb='FALSE', priv_own=1,
            acres=QC_LARGE_PARCEL_THRESHOLD + 1,
            name=NULL_OWNER_SENTINEL,  # contradictory but testing rule order
        )
        # Rule 2 fires first (XGB=FALSE, GIS=TRUE, priv_own=1, large)
        assert flag == 2

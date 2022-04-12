#!/usr/bin/env python3

"""Common utility functions for beancount importers."""

from beancount.core import data
from beancount.core.number import D


def usd_amount(dollars):
    """Amount in USD."""
    return data.Amount(D(dollars), 'USD')


def simple_usd_posting(account, dollars):
    """Create a simple posting in USD with no metadata or cost basis."""
    amount = usd_amount(dollars)
    return data.Posting(account, amount, None, None, None, None)


def simple_posting_pair(pos_acc, neg_acc, dollars):
    """Create a pair of simple postings.

    Add `dollars` to `pos_acc` and subtract them from `neg_acc`.
    """
    return [
        simple_usd_posting(pos_acc, dollars),
        simple_usd_posting(neg_acc, -dollars),
    ]

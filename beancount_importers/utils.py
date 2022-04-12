#!/usr/bin/env python3

"""Common utility functions for beancount importers."""

from beancount.core import data
from beancount.core.number import D

USD = 'USD'


def usd_amount(dollars):
    """Amount in USD."""
    return data.Amount(D(dollars), USD)


def simple_usd_posting(account, dollars, negative=False):
    """Create a simple posting in USD with no metadata or cost basis."""
    amount = usd_amount(dollars)
    if negative:
        amount = -amount
    return data.Posting(account, amount, None, None, None, None)


def simple_posting_pair(pos_acc, neg_acc, dollars):
    """Create a pair of simple postings.

    Add `dollars` to `pos_acc` and subtract them from `neg_acc`.
    """
    return [
        simple_usd_posting(pos_acc, dollars),
        simple_usd_posting(neg_acc, dollars, negative=True),
    ]


def open_usd_account(account_name, open_date):
    """Create an open directive for an account with only USD."""
    meta = data.new_metadata(None, None)
    return data.Open(
        meta,
        open_date,
        account_name,
        [USD],
        None,
        # data.Booking.STRICT,
    )


def open_usd_accounts(account_names, open_date):
    """Create an open directive for multiple accounts with only USD."""
    return [
        open_usd_account(account_name, open_date)
        for account_name in account_names
    ]
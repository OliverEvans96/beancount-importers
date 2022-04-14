#!/usr/bin/env python3

"""Common utility functions for beancount importers."""

from beancount.core import data
from beancount.core.number import D

USD = 'USD'


import hashlib
import uuid


def transaction_id(txn: data.Transaction):
    """Generate a unique and consistent id from a transaction's metadata."""
    h = hashlib.sha256()
    if txn.meta is not None:
        if 'filename' in txn.meta and 'lineno' in txn.meta:
            h.update(txn.meta['filename'].encode())
            h.update(txn.meta['lineno'].to_bytes(8, 'big'))
            d = h.digest()
            u = uuid.UUID(bytes=d[:16])
            return str(u)


def split_txn(txn):
    """Split a simple transaction into src and dst postings."""
    # This won't work for more complex transactions.
    assert len(txn.postings) == 2

    src_posting = None
    dst_posting = None

    for posting in txn.postings:
        if posting.units.number > 0:
            dst_posting = posting
        elif posting.units.number < 0:
            src_posting = posting

    return (src_posting, dst_posting)


def usd_amount(dollars):
    """Amount in USD."""
    return data.Amount(D(dollars), USD)

def simple_usd_posting(account, dollars, negative=False):
    """Create a simple posting in USD with no metadata or cost basis."""
    return simple_posting(account, dollars, negative)


def simple_posting(account, amount, negative=False, currency=USD):
    """Create a simple posting with no metadata or cost basis."""
    amount = data.Amount(D(amount), currency)
    if negative:
        amount = -amount
    return data.Posting(account, amount, None, None, None, None)


def simple_posting_pair(pos_acc, neg_acc, amount, currency='USD'):
    """Create a pair of simple postings.

    Add `dollars` to `pos_acc` and subtract them from `neg_acc`.
    """
    return [
        simple_posting(pos_acc, amount, currency=currency),
        simple_posting(neg_acc, amount, negative=True, currency=currency),
    ]


def blank_metadata():
    """Create a new metadata object with no source filename or line number."""
    filename, line_num = None, None
    meta = data.new_metadata(filename, line_num)
    return meta


def open_account(account_name, open_date, currencies=[USD]):
    """Create an open directive for an account."""
    meta = blank_metadata()
    return data.Open(
        meta,
        open_date,
        account_name,
        currencies,
        None,
        # data.Booking.STRICT,
    )


def open_accounts(account_names, open_date, currencies=[USD]):
    """Create an open directive for multiple accounts."""
    return [
        open_account(account_name, open_date, currencies)
        for account_name in account_names
    ]


def pad_account(account, date, src_account='Equity:OpeningBalances'):
    """Pad account balance from Equity:OpeningBalances."""
    meta = blank_metadata()
    return data.Pad(meta, date, account, src_account)

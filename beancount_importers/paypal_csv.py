#!/usr/bin/env python3

"""Beancount importer for paypal transactions CSV."""

import csv
import datetime
import enum
import os
import re

from beancount.core import data, flags
from beancount.core.number import D
from beancount.ingest import importer
from dateutil.parser import parse as parse_date

from .utils import open_account, open_accounts
from .utils import simple_posting_pair
from .utils import usd_amount
from .utils import pad_account

ENCODING = 'utf-8-sig'
DEFAULT_OPEN_DATE = datetime.date(2015, 1, 1)
DEFAULT_INCOME_ACC = 'Income:Uncategorized'
DEFAULT_EXPENSE_ACC = 'Expenses:Uncategorized'

class Account(enum.Enum):
    """Paypal accounts."""

    BAL = 'Assets:Paypal:Balance'
    DON = 'Expenses:Donations:Paypal'
    TRANS = 'Assets:InTransit:Paypal'


class Header(enum.Enum):
    """Paypal CSV headers."""

    DATE = 'Date'
    TIME = 'Time'
    TZ = 'TimeZone'
    NAME = 'Name'
    TYPE = 'Type'
    STAT = 'Status'
    CUR = 'Currency'
    AMT = 'Amount'
    ID = 'Receipt ID'
    BAL = 'Balance'


class TxnStatus(enum.Enum):
    """Paypal transaction statuses."""

    COMPLETED = 'Completed'
    DENIED = 'Denied'
    EXPIRED = 'Expired'
    PENDING = 'Pending'
    REVERSED = 'Reversed'


# class TxnType(enum.Enum):
#     """Paypal transactions."""

#     WITHDRAWAL = 'Withdrawal'
#     FIXED_PRICE = 'Fixed Price'
#     BONUS = 'Bonus'
#     HOURLY = 'Hourly'
#     REFUND = 'Refund'
#     SERVICE_FEE = 'Service Fee'
#     MISC = 'Miscellaneous'


class PaypalTransactionsImporter(importer.ImporterProtocol):
    """Paypal CSV transactions importer."""

    def __init__(self, clear_before_date, currencies=['USD'], open_date=DEFAULT_OPEN_DATE, pad=True):
        """Initialize."""
        self.open_date = open_date
        self.clear_before_date = clear_before_date
        self.currencies = currencies
        self.pad = pad

        # Maintain a list of unique transaction dates
        # to help construct balance assertions
        self.txn_dates = set()

    def file_account(self, file_cache):
        """Determine account related to this file."""
        return 'Assets:Paypal'

    def file_date(self, file_cache):
        """Determine the date related to this file."""
        filename_matches = re.match(
            '^paypal-transactions'
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # start date
            '_([0-9]{4}-[0-9]{2}-[0-9]{2})'  # end date
            '.CSV$',
            os.path.basename(file_cache.name)
        )

        if filename_matches is not None:
            groups = filename_matches.groups()
            if len(groups) > 0:
                date_str = groups[0]
                return parse_date(date_str).date()

    def identify(self, file_cache):
        """Determine whether a given file can be processed by this importer."""
        filename_matches = re.match(
            '^paypal-transactions'
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # start date
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # end date
            '.CSV$',
            os.path.basename(file_cache.name)
        )

        expected_headers = [
            hd.value for hd in Header
        ]

        # First check filename
        if filename_matches is not None:
            # If filename matches, check header
            with open(file_cache.name, encoding=ENCODING) as fh:
                reader = csv.reader(fh)
                headers = next(reader)
                is_valid = (headers == expected_headers)
                return is_valid
        else:
            return False

    def extract(self, file_cache):
        """Extract the transactions from the CSV."""
        open_entry = open_account(
            Account.BAL.value,
            self.open_date,
            self.currencies
        )
        other_open_entries = open_accounts([
            Account.DON.value,
            Account.TRANS.value,
        ], self.open_date, self.currencies)
        entries = [
            open_entry,
        ] + other_open_entries

        if self.pad:
            pad_entry = pad_account(Account.BAL.value, self.open_date)
            entries += pad_entry

        # Keep track of previous balances
        # (per-currency) for balance assertions
        prev_balances = {}

        with open(file_cache.name, encoding=ENCODING) as fh:
            for index, row in enumerate(csv.DictReader(fh)):
                txn_date = parse_date(row[Header.DATE.value]).date()
                txn_desc = (
                    row[Header.NAME.value].strip()
                    or row[Header.TYPE.value].strip()
                )
                txn_amt = row[Header.AMT.value].replace(',', '').strip()
                txn_cur = row[Header.CUR.value].strip()
                txn_type = row[Header.TYPE.value].strip()
                txn_status = TxnStatus(row[Header.STAT.value].strip())
                balance = row[Header.BAL.value]

                # Skip transactions with no amount
                if not txn_amt:
                    continue

                # Set fallback account
                if float(txn_amt) > 0:
                    src_acc = DEFAULT_INCOME_ACC
                elif float(txn_amt) < 0:
                    src_acc = DEFAULT_EXPENSE_ACC

                # Paypal bug? Some transactions don't seem to affect
                # the balance. Ignore such "ghost" transactions.
                if txn_cur in prev_balances:
                    if balance == prev_balances[txn_cur]:
                        continue

                # Skip certain types of "transactions"
                # that don't actually move funds
                skip_types = [
                    'Request Received',
                    'Request Sent'
                ]

                if txn_type in skip_types:
                    continue

                if txn_type == 'General Withdrawal':
                    if txn_date < self.clear_before_date:
                        dst_acc = 'Equity:Earnings:Previous'
                    else:
                        dst_acc = Account.TRANS.value

                elif txn_type == 'Donation Payment':
                    src_acc = Account.DON.value

                dst_acc = Account.BAL.value

                postings = simple_posting_pair(
                    dst_acc,
                    src_acc,
                    txn_amt,
                    currency=txn_cur,
                )

                meta_kwargs = {
                    'status': txn_status.value,
                    'type': txn_type,
                }

                meta = data.new_metadata(
                    file_cache.name,
                    index,
                    meta_kwargs.items()
                )
                txn = data.Transaction(
                    meta=meta,
                    date=txn_date,
                    flag=flags.FLAG_OKAY,
                    payee=None,
                    narration=txn_desc,
                    tags={'paypal'},
                    links=set(),
                    postings=postings,
                )
                entries.append(txn)

                # Assuming the transactions are in chronological order,
                # each time we encounter a new date, the previous
                # transaction's running balance should be today's
                # opening balance.
                if txn_date not in self.txn_dates:
                    # Record that we have encountered this date,
                    # so as to avoid duplicate / erroneous balance assertions
                    self.txn_dates.add(txn_date)

                    for cur, prev_balance in prev_balances.items():
                        balance_date = txn_date
                        meta = data.new_metadata(file_cache.name, index)
                        bal_amt = data.Amount(D(prev_balance), cur)
                        balance_entry = data.Balance(
                            meta,
                            balance_date,
                            Account.BAL.value,
                            bal_amt,
                            tolerance=None,
                            diff_amount=None,
                        )

                        entries.append(balance_entry)

                # Save this transaction's balance in case
                # it's the last for the day.
                prev_balances[txn_cur] = balance

        return entries

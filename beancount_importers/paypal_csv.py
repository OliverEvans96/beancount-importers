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
from .utils import simple_posting
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
    HELD = 'Assets:Paypal:Held'
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
            Account.HELD.value,
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

        # Used for combining currency conversion transactions
        first_conv_posting = None

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

                flag = flags.FLAG_WARNING
                more_tags = set()

                # Whether to skip outputting a transaction
                # (but finish the loop iteration)
                no_output = False

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
                    flag = flags.FLAG_OKAY

                elif txn_type in ['Payment Hold', 'Payment Release']:
                    src_acc = Account.HELD.value
                    flag = flags.FLAG_OKAY

                dst_acc = Account.BAL.value

                meta_kwargs = {
                    'status': txn_status.value,
                    'type': txn_type,
                }
                meta = data.new_metadata(
                    file_cache.name,
                    index,
                    meta_kwargs.items(),
                )

                # Combine currency conversions into single transactions
                if txn_type == 'General Currency Conversion':
                    if first_conv_posting is None:
                        # If the previous transaction was not a currency
                        # conversion transaction, then save this info
                        # but don't output a transaction.
                        first_conv_posting = simple_posting(
                            account=Account.BAL.value,
                            amount=txn_amt,
                            currency=txn_cur,
                        )
                        no_output = True
                    else:
                        # If this is the second consecutive conversion
                        # transaction, then use the previous one to
                        # output a single transaction now.
                        second_conv_posting = simple_posting(
                            account=Account.BAL.value,
                            amount=txn_amt,
                            currency=txn_cur,
                        )

                        # Add a price to the second posting
                        first_num = first_conv_posting.units.number
                        second_num = second_conv_posting.units.number
                        # Add the minus sign because the postings sum to zero.
                        conversion_rate = -first_num / second_num
                        second_conv_posting = second_conv_posting._replace(
                            price=data.Amount(
                                D(f'{conversion_rate:.5g}'),
                                first_conv_posting.units.currency
                            )
                        )

                        postings = [
                            first_conv_posting,
                            second_conv_posting,
                        ]
                        more_tags.add('currency-conversion')
                        flag = flags.FLAG_OKAY
                else:
                    # If this isn't a currency conversion
                    # transaction, then clear the cache.
                    first_conv_posting = None
                    postings = simple_posting_pair(
                        dst_acc,
                        src_acc,
                        txn_amt,
                        currency=txn_cur,
                    )

                if not no_output:
                    txn = data.Transaction(
                        meta=meta,
                        date=txn_date,
                        flag=flag,
                        payee=None,
                        narration=txn_desc,
                        tags={'paypal'} | more_tags,
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

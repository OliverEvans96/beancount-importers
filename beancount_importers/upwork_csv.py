#!/usr/bin/env python3

"""Beancount importer for upwork transactions CSV."""

import csv
import re
import os
from dateutil.parser import parse as parse_date
import datetime

from beancount.core import flags
from beancount.core import data

from beancount.ingest import importer

from .utils import simple_posting_pair
from .utils import usd_amount
from .utils import open_usd_accounts

UPWORK_ACC_BAL = 'Assets:Upwork:Balance'
UPWORK_ACC_FP = 'Income:Upwork:FixedPrice'
UPWORK_ACC_BON = 'Income:Upwork:Bonus'
UPWORK_ACC_HR = 'Income:Upwork:Hourly'
UPWORK_ACC_MISC = 'Income:Upwork:Miscellaneous'
UPWORK_ACC_SF = 'Expenses:Upwork:ServiceFee'
UPWORK_ACC_REF = 'Expenses:Upwork:Refund'

DEFAULT_OPEN_DATE = datetime.date(2018, 1, 1)


class UpworkTransactionsImporter(importer.ImporterProtocol):
    """Upwork CSV transactions importer."""

    def __init__(self, bank_account_dict, open_date=DEFAULT_OPEN_DATE):
        """Initialize."""
        self.bank_account_dict = bank_account_dict
        self.open_date = open_date

        # Maintain a list of unique transaction dates
        # to help construct balance assertions
        self.txn_dates = set()

    def identify(self, file_cache):
        """Determine whether a given file can be processed by this importer."""
        filename_matches = re.match(
            'statements'
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # start date
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # end date
            '.csv$',
            os.path.basename(file_cache.name)
        )

        expected_headers = [
            'Date',
            'Ref ID',
            'Type',
            'Description',
            'Agency',
            'Freelancer',
            'Team',
            'Account Name',
            'PO',
            'Amount',
            'Amount in local currency',
            'Currency',
            'Balance'
        ]

        # First check filename
        if filename_matches is not None:
            # If filename matches, check header
            with open(file_cache.name) as fh:
                reader = csv.reader(fh)
                headers = next(reader)
                is_valid = (headers == expected_headers)
                return is_valid
        else:
            return False

    def extract(self, file_cache):
        """Extract the transactions from the CSV."""
        open_accounts = [
            UPWORK_ACC_BAL,
            UPWORK_ACC_FP,
            UPWORK_ACC_BON,
            UPWORK_ACC_HR,
            UPWORK_ACC_MISC,
            UPWORK_ACC_SF,
            UPWORK_ACC_REF,
        ] + list(self.bank_account_dict.values())
        open_entries = open_usd_accounts(open_accounts, self.open_date)

        entries = open_entries

        with open(file_cache.name) as fh:
            for index, row in enumerate(csv.DictReader(fh)):
                txn_date = parse_date(row['Date']).date()
                txn_desc = row['Description']
                txn_amt = row['Amount']
                txn_type = row['Type']

                if txn_type == 'Withdrawal':
                    # Extract account number from transaction description
                    matches = re.match('.*: xxxx-([0-9]{4})', txn_desc)
                    if matches is not None and len(matches.groups()) > 0:
                        last_four = matches[1]
                    else:
                        msg = ("Could not extract acount number from "
                               f"Withdrawal description: '{txn_desc}'")
                        raise ValueError(msg)
                    dest_account = self.bank_account_dict[last_four]
                    # NOTE: sign of amount (positive or negative)
                    # depends on whether the transaction type
                    # increases or decreases the balance,
                    # which is why `UPWORK_ACC_BAL` is always
                    # the first argument to `simple_posting_pair`
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        dest_account,
                        txn_amt
                    )
                elif txn_type == 'Fixed Price':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_FP,
                        txn_amt
                    )
                elif txn_type == 'Bonus':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_BON,
                        txn_amt
                    )
                elif txn_type == 'Hourly':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_HR,
                        txn_amt
                    )
                elif txn_type == 'Refund':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_REF,
                        txn_amt
                    )
                elif txn_type == 'Service Fee':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_SF,
                        txn_amt
                    )
                elif txn_type == 'Miscellaneous':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_MISC,
                        txn_amt
                    )
                else:
                    msg = "Unknown transaction type: {}".format(txn_type)
                    raise ValueError(msg)

                meta = data.new_metadata(file_cache.name, index)
                txn = data.Transaction(
                    meta=meta,
                    date=txn_date,
                    flag=flags.FLAG_OKAY,
                    payee=None,
                    narration=txn_desc,
                    tags=set(),
                    links=set(),
                    postings=postings,
                )

                entries.append(txn)

                # Assuming the transactions are in reverse-chronological order,
                # the first transaction we encounter for a given day should be
                # chronologically the last, which means that the running
                # balance for that transaction should be the opening balance
                # balance on the following day.
                if txn_date not in self.txn_dates:
                    # Record that we have encountered this date,
                    # so as to avoid duplicate / erroneous balance assertions
                    self.txn_dates.add(txn_date)

                    balance = row['Balance']
                    balance_date = txn_date + datetime.timedelta(days=1)
                    balance_entry = data.Balance(
                        meta,
                        balance_date,
                        UPWORK_ACC_BAL,
                        usd_amount(balance),
                        None,
                        None,
                    )

                    entries.append(balance_entry)

        return entries

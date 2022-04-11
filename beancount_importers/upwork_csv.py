#!/usr/bin/env python3

"""Beancount importer for upwork transactions CSV."""

import csv
import re
import os
from dateutil.parser import parse as parse_date

from beancount.core import flags
from beancount.core import data
from beancount.core.number import D

from beancount.ingest import importer


UPWORK_ACC_BAL = 'Assets:Upwork:Balance'
UPWORK_ACC_FP = 'Income:Upwork:FixedPrice'
UPWORK_ACC_BON = 'Income:Upwork:Bonus'
UPWORK_ACC_HR = 'Income:Upwork:Hourly'
UPWORK_ACC_MISC = 'Income:Upwork:Miscellaneous'
UPWORK_ACC_SF = 'Expenses:Upwork:ServiceFee'
UPWORK_ACC_REF = 'Expenses:Upwork:Refund'


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


class UpworkTransactionsImporter(importer.ImporterProtocol):
    """Upwork CSV transactions importer."""

    def __init__(self, bank_account_dict):
        """Initialize."""
        self.bank_account_dict = bank_account_dict

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
        entries = []

        with open(file_cache.name) as fh:
            for index, row in enumerate(csv.DictReader(fh)):
                trans_date = parse_date(row['Date']).date()
                trans_desc = row['Description']
                trans_amt = float(row['Amount'])
                trans_type = row['Type']
                balance = row['Balance']

                if trans_type == 'Withdrawal':
                    # Extract account number from transaction description
                    matches = re.match('.*: xxxx-([0-9]{4})', trans_desc)
                    if matches is not None and len(matches.groups()) > 0:
                        last_four = matches[1]
                    else:
                        msg = ("Could not extract acount number from "
                               f"Withdrawal description: '{trans_desc}'")
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
                        trans_amt
                    )
                elif trans_type == 'Fixed Price':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_FP,
                        trans_amt
                    )
                elif trans_type == 'Bonus':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_BON,
                        trans_amt
                    )
                elif trans_type == 'Hourly':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_HR,
                        trans_amt
                    )
                elif trans_type == 'Refund':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_REF,
                        trans_amt
                    )
                elif trans_type == 'Service Fee':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_SF,
                        trans_amt
                    )
                elif trans_type == 'Miscellaneous':
                    postings = simple_posting_pair(
                        UPWORK_ACC_BAL,
                        UPWORK_ACC_MISC,
                        trans_amt
                    )
                else:
                    msg = "Unknown transaction type: {}".format(trans_type)
                    raise ValueError(msg)

                meta = data.new_metadata(file_cache.name, index)
                txn = data.Transaction(
                    meta=meta,
                    date=trans_date,
                    flag=flags.FLAG_OKAY,
                    payee=None,
                    narration=trans_desc,
                    tags=set(),
                    links=set(),
                    postings=postings,
                )

                entries.append(txn)

                # balance_entry = data.Balance(
                #     UPWORK_ACC_BAL,
                #     usd_amount(balance),
                #     None,
                #     None,
                # )
                # entries.append(balance_entry)

        return entries


#!/usr/bin/env python3

"""Beancount importer for upwork transactions CSV."""

import csv
import datetime
import enum
import os
import re

from beancount.core import data, flags
from beancount.ingest import importer
from dateutil.parser import parse as parse_date

from .utils import open_usd_accounts, simple_posting_pair, usd_amount


class Account(enum.Enum):
    """Upwork accounts."""

    BAL = 'Assets:Upwork:Balance'
    FP = 'Income:Upwork:FixedPrice'
    BON = 'Income:Upwork:Bonus'
    HR = 'Income:Upwork:Hourly'
    MISC = 'Income:Upwork:Miscellaneous'
    SF = 'Expenses:Upwork:ServiceFee'
    REF = 'Expenses:Upwork:Refund'


class Header(enum.Enum):
    """Upwork CSV headers."""

    DATE = 'Date'
    REF = 'Ref ID'
    TYPE = 'Type'
    DESC = 'Description'
    AGEN = 'Agency'
    FRLN = 'Freelancer'
    TEAM = 'Team'
    ACNT = 'Account Name'
    PO = 'PO'
    AMT = 'Amount'
    AMT_LC = 'Amount in local currency'
    CUR = 'Currency'
    BAL = 'Balance'


class TxnType(enum.Enum):
    """Upwork transactions."""

    WITHDRAWAL = 'Withdrawal'
    FIXED_PRICE = 'Fixed Price'
    BONUS = 'Bonus'
    HOURLY = 'Hourly'
    REFUND = 'Refund'
    SERVICE_FEE = 'Service Fee'
    MISC = 'Miscellaneous'


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
            '^statements'
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # start date
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # end date
            '.csv$',
            os.path.basename(file_cache.name)
        )

        expected_headers = [
            hd.value for hd in Header
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
            acc.value for acc in Account
        ] + list(self.bank_account_dict.values())
        open_entries = open_usd_accounts(open_accounts, self.open_date)

        entries = open_entries

        with open(file_cache.name) as fh:
            for index, row in enumerate(csv.DictReader(fh)):
                txn_date = parse_date(row[Header.DATE.value]).date()
                txn_desc = row[Header.DESC.value]
                txn_amt = row[Header.AMT.value]
                txn_type = row[Header.TYPE.value]

                if txn_type == TxnType.WITHDRAWAL.value:
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
                        Account.BAL.value,
                        dest_account,
                        txn_amt
                    )

                elif txn_type == TxnType.FIXED_PRICE.value:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.FP.value,
                        txn_amt
                    )

                elif txn_type == TxnType.BONUS.value:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.BON.value,
                        txn_amt
                    )

                elif txn_type == TxnType.HOURLY.value:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.HR.value,
                        txn_amt
                    )

                elif txn_type == TxnType.REFUND.value:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.REF.value,
                        txn_amt
                    )

                elif txn_type == TxnType.SERVICE_FEE.value:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.SF.value,
                        txn_amt
                    )

                elif txn_type == TxnType.MISC.value:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.MISC.value,
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
                        Account.BAL.value,
                        usd_amount(balance),
                        None,
                        None,
                    )

                    entries.append(balance_entry)

        return entries

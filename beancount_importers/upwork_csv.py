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


DEFAULT_OPEN_DATE = datetime.date(2018, 1, 1)
IN_TRANSIT_ACCOUNT_PREFIX = 'Assets:InTransit:Upwork'

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


class UpworkTxnTag(enum.Enum):
    """Beancount tags to apply to upwork transactions."""

    WITHDRAWAL = 'Upwork-Withdrawal'
    FIXED_PRICE = 'Upwork-FixedPrice'
    BONUS = 'Upwork-Bonus'
    HOURLY = 'Upwork-Hourly'
    REFUND = 'Upwork-Refund'
    SERVICE_FEE = 'Upwork-ServiceFee'
    MISC = 'Upwork-Miscellaneous'


def upwork_in_transit_account_name(last_four):
    """Create the full in-transit account name
    given the last four digits of the destination
    account number."""
    return f"{IN_TRANSIT_ACCOUNT_PREFIX}:Schwab-{last_four}"


def txn_type_to_tag(txn_type: TxnType):
    """Convert transaction type (from CSV) to beancount tag."""
    # NOTE: This assumes that `TxnType` and `UpworkTxnTag`
    # variant names are identical.
    return UpworkTxnTag.__getattr__(txn_type.name)


def get_last_four_from_upwork_description(txn_desc):
    """Extract the last four digits of destination account
    from Upwork withdrawal description."""
    matches = re.match('.*: xxxx-([0-9]{4})', txn_desc)
    if matches is not None and len(matches.groups()) > 0:
        last_four = matches[1]
        return last_four
    else:
        msg = ("Could not extract acount number from "
               f"Withdrawal description: '{txn_desc}'")
        raise ValueError(msg)


def get_account_from_upwork_description(txn_desc, account_dict):
    """Extract beancount account from upwork transaction description."""
    # Extract account number from transaction description
    last_four = get_last_four_from_upwork_description(txn_desc)
    dest_account = account_dict[last_four]
    return dest_account


class UpworkTransactionsImporter(importer.ImporterProtocol):
    """Upwork CSV transactions importer."""

    def __init__(self, bank_account_dict, open_date=DEFAULT_OPEN_DATE):
        """Initialize."""
        self.bank_account_dict = bank_account_dict
        self.open_date = open_date

        # Maintain a list of unique transaction dates
        # to help construct balance assertions
        self.txn_dates = set()

    def file_account(self, file_cache):
        """Determine account related to this file."""
        return 'Assets:Upwork'

    def file_date(self, file_cache):
        """Determine the date related to this file."""
        filename_matches = re.match(
            '^statements'
            '_[0-9]{4}-[0-9]{2}-[0-9]{2}'  # start date
            '_([0-9]{4}-[0-9]{2}-[0-9]{2})'  # end date
            '.csv$',
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
        in_transit_accounts = [
            upwork_in_transit_account_name(last_four)
            for last_four in self.bank_account_dict.keys()
        ]
        open_accounts = [
            acc.value for acc in Account
        ] + in_transit_accounts
        open_entries = open_usd_accounts(open_accounts, self.open_date)

        entries = open_entries

        with open(file_cache.name) as fh:
            for index, row in enumerate(csv.DictReader(fh)):
                txn_date = parse_date(row[Header.DATE.value]).date()
                txn_desc = row[Header.DESC.value]
                txn_amt = row[Header.AMT.value]
                txn_type = TxnType(row[Header.TYPE.value])

                if txn_type == TxnType.WITHDRAWAL:

                    last_four = get_last_four_from_upwork_description(txn_desc)
                    dest_account = upwork_in_transit_account_name(last_four)

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

                elif txn_type == TxnType.FIXED_PRICE:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.FP.value,
                        txn_amt
                    )

                elif txn_type == TxnType.BONUS:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.BON.value,
                        txn_amt
                    )

                elif txn_type == TxnType.HOURLY:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.HR.value,
                        txn_amt
                    )

                elif txn_type == TxnType.REFUND:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.REF.value,
                        txn_amt
                    )

                elif txn_type == TxnType.SERVICE_FEE:
                    postings = simple_posting_pair(
                        Account.BAL.value,
                        Account.SF.value,
                        txn_amt
                    )

                elif txn_type == TxnType.MISC:
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
                    tags={txn_type_to_tag(txn_type).value},
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

                    balance = row[Header.BAL.value]
                    balance_date = txn_date + datetime.timedelta(days=1)
                    balance_entry = data.Balance(
                        meta,
                        balance_date,
                        Account.BAL.value,
                        usd_amount(balance),
                        tolerance=None,
                        diff_amount=None,
                    )

                    entries.append(balance_entry)

        return entries

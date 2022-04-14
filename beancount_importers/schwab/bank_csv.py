#!/usr/bin/env python3

"""Beancount importer for schwab transactions CSV."""

import csv
import re
import os
from dateutil.parser import parse as parse_date
import datetime
import enum

from beancount.core import flags
from beancount.core import data

from beancount.ingest import importer

from ..utils import simple_posting_pair
from ..utils import usd_amount
from ..utils import open_usd_accounts
from ..utils import pad_account

DEFAULT_OPEN_DATE = datetime.date(2016, 1, 1)
DEFAULT_EXPENSE_ACC = 'Expenses:Uncategorized'
DEFAULT_INCOME_ACC = 'Income:Uncategorized'

class Header(enum.Enum):
    """Schwab bank CSV headers."""

    DATE = 'Date'
    TYPE = 'Type'
    CHK_NUM = 'Check #'
    DESC = 'Description'
    WDL = 'Withdrawal (-)'
    DEP = 'Deposit (+)'
    BAL = 'RunningBalance'


class TxnType(enum.Enum):
    """Schwab bank transaction types."""

    ACH = 'ACH'
    ATM = 'ATM'
    ATM_REBATE = 'ATMREBATE'
    CHECK = 'CHECK'
    DEPOSIT = 'DEPOSIT'
    INT_ADJUST = 'INTADJUST'
    TRANSFER = 'TRANSFER'
    VISA = 'VISA'
    WIRE = 'WIRE'


# These lines contain junk
# (counting from first line = 1)
SKIP_LINES = [1, 3, 4]

def read_records(filename, skip_lines=SKIP_LINES):
    """Read CSV lines (including the header).

    Skip lines that are known to be irrelevant.
    Yields (line_num, records) for each line."""

    with open(filename) as fh:
        reader = csv.reader(fh)
        for i, fields in enumerate(reader):
            if i+1 in skip_lines:
                continue
            else:
                yield i+1, fields

# def dict_reader_from_records(line_iter):
#     """Transform csv.reader output into csv.DictReader output.

#     Given an iterator over lists of strings, return an iterator
#     over dicts, mapping headers to field values.

#     It is assumed that the first line contains headers,
#     and that each line contains the same number of fields."""
#     headers = next(line_iter)
#     for fields in line_iter:
#         yield dict(zip(headers, fields))


class SchwabBankTransactionsImporter(importer.ImporterProtocol):
    """Schwab checking account CSV transactions importer."""

    def __init__(self, account_prefix='Assets:Schwab', open_date=DEFAULT_OPEN_DATE):
        """Initialize."""
        self.account_prefix = account_prefix
        self.open_date = open_date

        # Maintain a list of unique transaction dates
        # to help construct balance assertions
        self.txn_dates = set()

    def file_account(self, file_cache):
        """Determine account related to this file.

        e.g. Assets:Schwab:PersonalChecking."""
        acc_name = self._file_account_suffix(file_cache)
        if acc_name is not None:
            return self._create_account_name(acc_name)

    def _create_account_name(self, account_suffix):
        """Join account prefix with suffix to construct full name.

        e.g. Assets:Schwab:PersonalChecking."""
        return ':'.join([
            self.account_prefix,
            account_suffix
        ])

    def _file_account_suffix(self, file_cache):
        """Determine the account suffix name (full name without prefix).

        e.g. PersonalChecking."""
        filename_matches = re.match(
            '(^[A-z].*)_'  # account name
            'Transactions_'
            '[0-9]{8}-'  # access date
            '[0-9]*'  # report number?
            '.CSV$',
            os.path.basename(file_cache.name)
        )

        if filename_matches is not None:
            groups = filename_matches.groups()
            if len(groups) > 0:
                # Extract account name from the filename
                orig_acc_name = groups[0]
                # Remove underscores from the name
                final_acc_name = orig_acc_name.replace('_', '')

                return final_acc_name


    def identify(self, file_cache):
        """Determine whether a given file can be processed by this importer."""
        filename_matches = re.match(
            '^[A-z]*_Checking_'  # account name
            'Transactions_'
            '[0-9]{8}-'  # access date
            '[0-9]*'  # report number?
            '.CSV$',
            os.path.basename(file_cache.name)
        )

        expected_headers = [hd.value for hd in Header]

        # First check filename
        if filename_matches is not None:
            # If filename matches, check header
            for i, headers in read_records(file_cache.name):
                is_valid = (headers == expected_headers)
                return is_valid
        else:
            return False

    def extract(self, file_cache):
        """Extract the transactions from the CSV."""

        account_suffix = self._file_account_suffix(file_cache)
        account_name = self._create_account_name(account_suffix)

        atm_rebate_acc = ':'.join([
            'Income:Schwab:AtmRebate',
            account_suffix
        ])

        interest_acc = ':'.join([
            'Income:Schwab:Interest',
            account_suffix
        ])

        open_entries = open_usd_accounts([
            # TODO : Clean this up
            account_name,
            # DEFAULT_EXPENSE_ACC,
            # DEFAULT_INCOME_ACC,
            atm_rebate_acc,
            interest_acc,
            # 'Equity:OpeningBalances'
        ], self.open_date)

        pad_entry = pad_account(account_name, self.open_date)

        entries = open_entries + [pad_entry]

        records_reader = read_records(file_cache.name)
        # Assume the first line returned contains headers
        (header_index, headers) = next(records_reader)

        for index, records in records_reader:
            # Assume each line has the same number of fields
            row = dict(zip(headers, records))

            txn_date = parse_date(row[Header.DATE.value]).date()
            txn_desc = row[Header.DESC.value]
            txn_type = row[Header.TYPE.value]

            # Amount is split into two parts (both always positive)
            # Remove dollar sign
            txn_wdl = row[Header.WDL.value].replace('$', '')
            txn_dep = row[Header.DEP.value].replace('$', '')

            # Set default accounts if auto-categorization fails
            src_acc = DEFAULT_INCOME_ACC
            dst_acc = DEFAULT_EXPENSE_ACC

            # Default flag if not auto-categorized
            flag = flags.FLAG_WARNING

            if txn_type == TxnType.ACH.value:
                pass
            elif txn_type == TxnType.ATM.value:
                pass
            elif txn_type == TxnType.ATM_REBATE.value:
                src_acc = atm_rebate_acc
                flag = flags.FLAG_OKAY
            elif txn_type == TxnType.CHECK.value:
                pass
            elif txn_type == TxnType.DEPOSIT.value:
                pass
            elif txn_type == TxnType.INT_ADJUST.value:
                src_acc = interest_acc
                flag = flags.FLAG_OKAY
            elif txn_type == TxnType.TRANSFER.value:
                # TODO: Extract other account from description
                pass
            elif txn_type == TxnType.VISA.value:
                pass
            elif txn_type == TxnType.WIRE.value:
                pass
            else:
                msg = "Unknown transaction type: {}".format(txn_type)
                raise ValueError(msg)


            # Construct posting with appropriate values & accounts
            # Withdrawal
            if txn_wdl and not txn_dep:
                src_acc = account_name
                postings = simple_posting_pair(
                    dst_acc,
                    src_acc,
                    txn_wdl
                )
            # Deposit
            elif not txn_wdl and txn_dep:
                dst_acc = account_name
                postings = simple_posting_pair(
                    dst_acc,
                    src_acc,
                    txn_dep
                )
            else:
                msg = ' '.join([
                    "Line {}:".format(index),
                    "Expected exactly one of '{}' and '{}' to be present,".format(
                        Header.WDL.value,
                        Header.DEP.value,
                    ),
                    "but found ({}, {})".format(
                        txn_wdl,
                        txn_dep
                    ),
                ])
                raise ValueError(msg)

            meta = data.new_metadata(file_cache.name, index)
            txn = data.Transaction(
                meta=meta,
                date=txn_date,
                flag=flag,
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

                # # Create an extra balance assertion before the
                # # first transaction so that pads work correctly
                # # after opening the account.
                # if

                # Remove dollar sign
                balance = row[Header.BAL.value].replace('$', '')
                balance_date = txn_date + datetime.timedelta(days=1)
                balance_entry = data.Balance(
                    meta,
                    balance_date,
                    account_name,
                    usd_amount(balance),
                    tolerance=None,
                    diff_amount=None,
                )

                entries.append(balance_entry)

        return entries

import requests
import zipfile
import io
import re
import dataflows as DF
from table import Table, color_scheme_red_green

YEAR = 2025
WEIRD_ZIP_FILE = f'https://next.obudget.org/datapackages/budget/national/changes/finance-committee.zip'
DIGITS_RE = re.compile(r'([-\d]+)')
CHARS = 'אבגדהוזחטיךכלםמןנסעףפץצקרשת'

def get_outstanding_requests():
    requests_ids = []
    response = requests.get(WEIRD_ZIP_FILE)
    response.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(response.content), metadata_encoding='iso-8859-8') as zf:
        for filename in zf.namelist():
            filename = [ord(x) for x in filename]
            filename = [chr(x) if x < 0x80 else CHARS[x - 0x80] for x in filename]
            filename = ''.join(filename)
            # d = detect(filename)
            # filename = filename.decode('MacCyrillic', errors='ignore')
            # print(filename)
            # get all digits from the filename and add to requests_ids:
            digits = DIGITS_RE.findall(filename)
            for digit in digits:
                digit = digit.strip('-')
                digit = digit.split('-')
                digit = [int(x) for x in digit]
                if len(digit) == 2:
                    digit = list(range(digit[0], digit[1] + 1))
                for item in digit:
                    if item not in requests_ids:
                        requests_ids.append(item)
            # requests_ids.extend(digits)

    print('Requests IDs:', requests_ids)
    return requests_ids


CHANGES_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/changes/raw-budget-changes-enriched/datapackage.json'
BUDGET_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/processed/with-extras/datapackage.json'
HISTORY_YEARS = 4
# CONNECTED_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/processed/connected-items-explained/datapackage.json'
# MIN_YEAR = 2021

CHECKPOINT_DIR = '.checkpoints/budget-changes'

def get_change_per_year_no_surplus():
    rows = DF.Flow(
        DF.load(CHANGES_SOURCE_DATAPACKAGE, resources=['national-budget-changes']),
        DF.checkpoint('changes-raw', CHECKPOINT_DIR),
        DF.filter_rows(lambda row: row['year'] >= YEAR - HISTORY_YEARS),
        DF.filter_rows(lambda row: row['change_title'] != 'עודפים'),
        DF.join_with_self('national-budget-changes', ['budget_code', 'year'], dict(
            budget_code=None,
            year=None,
            net_expense_diff=dict(aggregate='sum'),
        )),
        DF.checkpoint('change_per_year_no_surplus', CHECKPOINT_DIR),
    ).results()[0][0]
    mapping = dict(
        ((row['year'], row['budget_code']), row['net_expense_diff'])
        for row in rows
        if row['net_expense_diff'] != 0
    )
    return mapping

def get_original_budget_per_program():
    rows = DF.Flow(
        DF.load(BUDGET_SOURCE_DATAPACKAGE),
        DF.checkpoint('budget-raw', CHECKPOINT_DIR),
        DF.filter_rows(lambda row: row['year'] >= YEAR - HISTORY_YEARS),
        DF.filter_rows(lambda row: row['code'] < '0089'),
        DF.filter_rows(lambda row: not row['code'].startswith('0000')),
        DF.filter_rows(lambda row: not row['code'].startswith('C')),
        DF.filter_rows(lambda row: len(row['code']) == 8),
        DF.select_fields(['year', 'code', 'net_allocated']),
        DF.checkpoint('budget_per_program', CHECKPOINT_DIR),
    ).results()[0][0]
    mapping = dict(
        ((row['year'], row['code']), row['net_allocated'])
        for row in rows
        if row['net_allocated'] not in (0, None)
    )
    return mapping

def get_parent_names_per_program():
    rows = DF.Flow(
        DF.load(BUDGET_SOURCE_DATAPACKAGE),
        DF.checkpoint('budget-raw', CHECKPOINT_DIR),
        DF.filter_rows(lambda row: row['year'] == YEAR),
        DF.filter_rows(lambda row: row['code'] < '0089'),
        DF.filter_rows(lambda row: not row['code'].startswith('0000')),
        DF.filter_rows(lambda row: not row['code'].startswith('C')),
        DF.filter_rows(lambda row: len(row['code']) in (4,6)),
        DF.select_fields(['code', 'title']),
        DF.checkpoint('parent_names_per_program', CHECKPOINT_DIR),
    ).results()[0][0]
    mapping = dict(
        (row['code'], row['title'])
        for row in rows
    )
    return mapping

def get_changes(requests_nos):
    rows = DF.Flow(
        DF.load(CHANGES_SOURCE_DATAPACKAGE, resources=['national-budget-changes']),
        DF.checkpoint('changes-raw', CHECKPOINT_DIR),
        DF.filter_rows(lambda row: row['year'] == YEAR),
        DF.filter_rows(lambda row: row['budget_code'] < '0089'),
        DF.set_type('committee_id', type='string', transform=first_item),
        DF.add_field('key', type='string', default=lambda row: f"{row['committee_id']:05d}" if row['committee_id'] else row['transaction_id']),
        DF.add_field('sort_key', type='string', default=lambda row: f"{row['committee_id']:05d}-{row['budget_code']}" if row['committee_id'] else row['transaction_id']),
        DF.sort_rows('{sort_key}'),
        # DF.printer()
    ).results()[0][0]
    return rows
    
def first_item(items):
    if isinstance(items, list):
        if len(items) > 0:
            return items[0]
        else:
            return None
    return items

def construct_table():
    # process_data()
    # outstanding_requests = get_outstanding_requests()
    outstanding_requests = None
    change_per_year_no_surplus = get_change_per_year_no_surplus()
    original_budget_per_program = get_original_budget_per_program()
    parent_names_per_program = get_parent_names_per_program()
    changes = get_changes(outstanding_requests)
    # explanations = get_explanations(outstanding_requests)
    # print('OUTSTANDING REQUESTS:', changes)
    color_index = 0
    BLUE_SCHEME = [
        {'color': 'FFFFFF', 'background_color': '0d0f42'},
        {'color': 'FFFFFF', 'background_color': '141664'},
        {'color': 'FFFFFF', 'background_color': '1b1d85'},
        {'color': 'FFFFFF', 'background_color': '2125a6'},
        {'color': 'FFFFFF', 'background_color': '282cc7'},
        {'color': 'FFFFFF', 'background_color': '4c4fd0'},
        {'color': 'FFFFFF', 'background_color': '7072da'},
        {'color': 'FFFFFF', 'background_color': '9395e3'},
        {'color': '222446', 'background_color': 'b7b9ec'},
    ]
    ORANGE_BG = {
        'color': 'FFFFFF',
        'background_color': 'FF6432'
    }
    LG_BG = {
        'color': '222446',
        'background_color': 'FBFFF3'
    }
    RED_GREEN_LG_BG = {
        'color': '222446',
        'background_color': color_scheme_red_green('FBFFF3')
    }
    DG_BG = {
        'color': '222446',
        'background_color': 'cbf99f'
    }
    BLUE_BG = {
        'color': lambda _: BLUE_SCHEME[color_index % len(BLUE_SCHEME)]['color'],
        'background_color': lambda _: BLUE_SCHEME[color_index % len(BLUE_SCHEME)]['background_color']
    }

    ROW_FIELDS = [
        {
            'title': 'מספר פנייה',
            'key': lambda row: first_item(row['committee_id']),
            'options': ORANGE_BG,
        },
        {
            'title': 'קוד סעיף',
            'key': lambda row: row['budget_code'][2:4],
            'options': BLUE_BG
        },
        {
            'title': 'שם סעיף',
            'key': lambda row: parent_names_per_program[row['budget_code'][:4]],
            'options': BLUE_BG
        },
        {
            'title': 'קוד תחום',
            'key': lambda row: row['budget_code'][2:6],
            'options': BLUE_BG
        },
        {
            'title': 'שם תחום',
            'key': lambda row: parent_names_per_program[row['budget_code'][:6]],
            'options': BLUE_BG
        },
        {
            'title': 'קוד תכנית',
            'key': lambda row: row['budget_code'][2:8],
            'options': BLUE_BG
        },
        {
            'title': 'שם תכנית',
            'key': lambda row: row['budget_title'],
            'options': BLUE_BG
        },
        {
            'title': 'הסבר לתכנית',
            'key': lambda row: row['budget_item_description'],
            'options': dict(BLUE_BG, align='right')
        },
        {
            'title': 'בקשת השינוי הוצאה נטו במלש"ח',
            'key': lambda row: row['net_expense_diff'] / 1000000,
            'options': dict(LG_BG, number_format='#,##0.0')
        },
        {
            'title': 'תיאור בקשה',
            'key': lambda row: first_item(row['req_title']),
            'options': LG_BG
        },
        {
            'title': 'שם שינוי',
            'key': lambda row: first_item(row['change_title']),
            'options': LG_BG
        },
        {
            'title': 'מטרת השינוי - מדברי ההסבר',
            'key': lambda row: row['change_explanation'],
            'options': dict(LG_BG, align='right')
        },
        {
            'title': f'מקורי {YEAR}',
            'key': lambda row: original_budget_per_program.get((YEAR, row['budget_code']), 0) / 1000000,
            'options': dict(DG_BG, number_format='#,##0.0')
        },    
        {
            'title': 'שיעור השינוי המבוקש',
            'key': lambda row: (row['net_expense_diff'] / original_budget_per_program.get((YEAR, row['budget_code']), 1)),
            'options': dict(RED_GREEN_LG_BG, number_format='0%')
        },
        *sum(
            (
                [
                    {
                        'title': f'מקורי {year}',
                        'key': lambda row, year=year: original_budget_per_program.get((year, row['budget_code']), 0) / 1000000,
                        'number_format': '#,##0.0',
                        'options': DG_BG
                    },
                    {
                        'title': f'מאושר מנוכה עודפים {year}',
                        'key': lambda row, year=year: (original_budget_per_program.get((year, row['budget_code']), 0) + change_per_year_no_surplus.get((year, row['budget_code']), 0)) / 1000000,
                        'number_format': '#,##0.0',
                        'options': LG_BG
                    }
                ]
                for year in range(YEAR-1, YEAR - HISTORY_YEARS-1, -1)
            ), start=[]
        )
    ]

    t = Table('שינויים לשנה השוטפת', None, None)
    rowkey = None
    for row in changes:
        _rowkey = row['key']
        if not _rowkey:
            continue
        if rowkey:
            if rowkey != _rowkey:
                color_index += 1
        rowkey = _rowkey
        t.new_row(rowkey)
        for i, field in enumerate(ROW_FIELDS):
            key = field['key']
            value = key(row)
            options = dict(field.get('options', {}))
            if not options.get('align'):
                options['align'] = 'center'
            if callable(options['color']):
                options['color'] = options['color'](value)
            if callable(options['background_color']):
                options['background_color'] = options['background_color'](value)
            # print(f'ROW KEY: {row_key}, FIELD: {field["title"]}, VALUE: {value}')
            t.set(field['title'], value, i, **options)
    t.save('budget-changes.xlsx')

if __name__=='__main__':
    construct_table()

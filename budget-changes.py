import json
import requests
import zipfile
import io
import re
import dataflows as DF
from table import Table, BG_COLOR_NAMES, color_scheme_red_green
import copy
from openai import OpenAI

YEAR = 2024
WEIRD_ZIP_FILE = f'https://next.obudget.org/datapackages/budget/national/changes/finance-committee.zip'
DIGITS_RE = re.compile(r'([-\d]+)')
CHARS = 'אבגדהוזחטיךכלםמןנסעףפץצקרשת'
API_KEY = open('OPENAI_APIKEY').read().strip()

client = OpenAI(api_key=API_KEY)

def explain_single_item():
    def func(row):
        PROMPT = """
You are a data analyst. You have been given details of a budget change request. 
The budget change request contains many changes - your job is to provide a concise explanation of a single change.
Your sources are: The details of the budget change request, the explanatory notes attached to the request and the details of the single change itself.

The details of the budget change request are:
"""
        summary_prompt = []
        summary = row['summary']
        summary_prompt.append(f'Request Title: {summary["title"]}')
        summary_prompt.append(f'Change Kind: {summary["kind"]}')
        if summary.get('from'):
            from_items = summary['from']
            from_items = '\n- '.join('{} ({})'.format(x[2], x[0][2:]) for x in from_items)
            summary_prompt.append(f'From:\n- {from_items}')
        if summary.get('to'):
            to_items = summary['to']
            to_items = '\n- '.join('{} ({})'.format(x[2], x[0][2:]) for x in to_items)
            summary_prompt.append(f'To:\n- {to_items}')
        PROMPT += '\n'.join(summary_prompt)
        if row.get('explanation'):
            PROMPT += f"""

The explanatory notes that were provided with the request are:
{row['explanation']}
"""
        if row.get('change_list'):
            change_list = row['change_list'][0]
            change_list_prompt = []
            for k, v in change_list.items():
                if 'diff' in k and v != 0:
                    k = k.replace('_', ' ').capitalize() + '.: '
                    change_list_prompt.append(f'{k}{v}')
            change_list_prompt = '\n- '.join(change_list_prompt)
            code, title = change_list['budget_code_title'].split(':', 1)
            PROMPT += f"""
The details of the change itself are:
- CODE: {code[2:]}
- TITLE: {title}
- {change_list_prompt}

Please provide:
1. A concise, one sentence explanation of the above change, in Hebrew.
   Don't focus on what the change is, but on why it was made. Be as exact and specific as possible, while still being concise and clear and including all relevant details.
2. A description of the budget item itself, as found verbatim in the explanatory notes (usually comes after the words "תיאור התוכנית:").
   Don't paraphrase or summarize it, but provide it as is, and only use the text in the explanatory notes (don't include the words "תיאור התוכנית:")
   If such a description is not available, say "UNAVAILABLE".

Provide your answer in JSON format. The JSON object should look like this:
{{
    "explanation": "The explanation of the change",
    "description": "The description of the budget item itself"
}}

Do not include any other information, embellishments or explanations, but only the JSON object itself.

"""
            
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": [
                        { "type": "text", "text": PROMPT },
                    ],
                }
            ],
            response_format=dict(type='json_object'),
        )

        content = completion.choices[0].message.content
        content = json.loads(content)
        row['explanation'] = content['explanation']
        row['item_description'] = content['description']
        print('EXPLANATION:', content['explanation'])
        if row['item_description'] == 'UNAVAILABLE':
            row['item_description'] = ''
        else:
            if content['description'] not in PROMPT:
                print('ROUGE DESCRIPTION:', content['description'])

    return DF.Flow(
        DF.add_field('item_description', type='string'),
        func,
    )


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


CHANGES_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/changes/with-transaction-id/datapackage.json'
CHANGES_EXPLANATIONS_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/changes/full/datapackage.json'
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
        DF.filter_rows(lambda row: row['committee_id'] in requests_nos),
        DF.sort_rows('transaction_id'),
        # DF.printer()
    ).results()[0][0]
    return rows
    
def get_explanations(requests_nos):

    def unwind_codes():
        def func(rows):
            for row in rows:
                if row['budget_code_title']:
                    for item in row['budget_code_title']:
                        code, title = item.split(':', 1)
                        out = copy.copy(row)
                        out.update(dict(
                            code=code,
                            title=title,
                            change_list=[x for x in row['change_list'] if x['budget_code_title'] == item],
                        ))
                        yield out
        return DF.Flow(
            DF.add_field('code', type='string'),
            DF.add_field('title', type='string'),
            func,
            DF.delete_fields(['budget_code_title']),
        )

    rows = DF.Flow(
        DF.load(CHANGES_EXPLANATIONS_SOURCE_DATAPACKAGE, resources=['national-budget-changes']),
        DF.checkpoint('changes-explanations-raw', CHECKPOINT_DIR),
        DF.filter_rows(lambda row: row['year'] == YEAR),
        DF.filter_rows(lambda row: any(i in requests_nos for i in row['committee_id'])),    
        DF.select_fields(['summary', 'budget_code_title', 'committee_id', 'explanation', 'change_list']),
        unwind_codes(),
        explain_single_item(),
        DF.checkpoint('changes-explanations', CHECKPOINT_DIR),
        DF.printer()
    ).results()[0][0]

    explanations = dict()
    for row in rows:
        if row['explanation']:
            row['explanation'] = row['explanation'].replace('\n', ' ')
            row['explanation'] = row['explanation'].replace('  ', ' ')
            row['explanation'] = row['explanation'].strip()
        for committee_id in row['committee_id']:
            key = (committee_id, row['code'])
            explanations[key] = [row['explanation'], row['item_description']]
    return explanations
    

# def nice_code(code):
#     return code[2:]
#     # With dots:
#     # ret = []
#     # code = code[2:]
#     # return f'="{code}"'
#     # while code:
#     #     ret.append(code[:2])
#     #     code = code[2:]
#     # return '.'.join(ret)

# def check_for_active(row):
#     if (row['net_allocated'] or row['net_revised'] or row['net_executed']):
#         return True
#     history = row.get('history') or {}
#     for year, rec in history.items():
#         year = int(year)
#         if year >= MIN_YEAR:
#             if rec.get('net_allocated') or rec.get('net_revised') or rec.get('net_executed'):
#                 return True
#     return False

# def get_proposal_data():
#     raw = DF.Flow(
#         DF.load(BUDGET_SOURCE_DATAPACKAGE),
#         DF.filter_rows(lambda row: row['year'] >= MIN_YEAR),
#         DF.filter_rows(lambda row: not row['code'].startswith('0000')),
#         DF.filter_rows(lambda row: not row['code'].startswith('C')),
#         DF.filter_rows(lambda row: row['code'] < '0089'),
#         DF.filter_rows(lambda row: len(row['code']) >= 4),
#         DF.select_fields([
#             'code',
#             'year',
#             'title',
#             'non_repeating',
#             'net_allocated',
#             'net_revised',
#             'net_executed',
#         ]),
#         DF.set_type('code', transform=nice_code),
#         DF.checkpoint('raw', CHECKPOINT_DIR),
#     ).results()[0][0]
#     raw_map = dict(
#         ((row['year'], row['code']), row)
#         for row in raw
#     )
#     connected = DF.Flow(
#         DF.load(CONNECTED_SOURCE_DATAPACKAGE),
#         DF.filter_rows(lambda row: row['year'] >= MIN_YEAR),
#         DF.filter_rows(lambda row: not row['code'].startswith('0000')),
#         DF.filter_rows(lambda row: not row['code'].startswith('C')),
#         DF.filter_rows(lambda row: row['code'] < '0089'),
#         DF.filter_rows(lambda row: len(row['code']) >= 4),
#         DF.filter_rows(check_for_active),
#         # DF.filter_rows(lambda row: (len(row['code']) < 10) or (row['net_allocated'] or row['net_revised'] or row['net_executed'])),
#         DF.select_fields([
#             'code',
#             'year',
#             'title',
#             'history',
#             'hierarchy',
#             'is_proposal',
#         ]),
#         DF.set_type('code', transform=nice_code),
#         DF.checkpoint('connected', CHECKPOINT_DIR),
#     ).results()[0][0]
#     return raw_map, connected

# def process_data():
#     raw_map, connected = get_proposal_data()
#     max_year = max(r['year'] for r in connected)
#     proposal_year = list(set(r['year'] for r in connected if r['is_proposal']))
#     proposal_year = proposal_year[0] if len(proposal_year) == 1 else None

#     if proposal_year is None:
#         raise Exception('Could not find a single proposal year, bailing out')
#     before_proposal_year = proposal_year - 1

#     used_keys = set()
#     # table_rows = list()
#     # headers = dict()

#     titles_for_code_aux = dict()
#     for item in raw_map.values():
#         if item.get('net_allocated') or item.get('net_revised') or item.get('net_executed'):
#             titles_for_code_aux.setdefault(item['code'], dict()).setdefault(item['title'], set()).add(item['year'])
#     for k in titles_for_code_aux:
#         titles_for_code_aux[k] = sorted((max(y), t) for t, y in titles_for_code_aux[k].items())
#     titles_for_code = dict(
#         (k, v[-1])
#         for k, v in titles_for_code_aux.items()
#     )
#     titles_comments_for_code = dict()
#     for k, v in titles_for_code_aux.items():
#         if len(v) > 1:
#             titles_comments_for_code[k] = ', '.join(f'עד שנת {y} נקרא {t}' for y, t in v[:-1])
#             print('TFC', k, v)

#     table = Table('השוואת הצעת התקציב', 
#                   group_fields=['קוד סעיף', 'קוד תחום', 'קוד תכנית'],
#                   cleanup_fields=['קוד סעיף', 'שם סעיף', 'קוד תחום', 'שם תחום', 'קוד תכנית', 'שם תכנית'])

#     for year in range(max_year, MIN_YEAR-1, -1):
#         print(f'PROCESSING YEAR {year}, got so far {len(used_keys)} keys')
#         for item in connected:
#             if item['year'] != year:
#                 continue
#             code = item['code']
#             title = item['title']
#             # max_year_, title = titles_for_code[code]
#             # if item['year'] != max_year:
#             #     print(f'Item {code} is not max year {item["year"]} != {max_year}')
#             #     continue
#             # print(f'Processing {year} {code}')
#             key = (year, code)
#             if key in used_keys:
#                 continue
#             keys = [(year, [code])]
#             history = item['history']
#             if history is not None:
#                 history = list((int(k), v) for k, v in history.items())
#                 history = sorted(history, key=lambda x: x[0], reverse=True)
#                 for _year, _rec in history:
#                     if _year < MIN_YEAR:
#                         break
#                     if _rec.get('net_allocated') or _rec.get('net_revised') or _rec.get('net_executed'):
#                         _year_codes = [nice_code(x.split(':')[0]) for x in _rec['code_titles']]
#                         keys.append((_year, _year_codes))
#             # print('KEYS', keys)

#             hierarchy = item['hierarchy']
#             code_titles = [(nice_code(h[0]), titles_for_code[nice_code(h[0])][1], None) for h in hierarchy[1:]] + [(code, title, titles_comments_for_code.get(code))]

#             row_key = (code, year)
#             table.new_row(row_key)

#             table.set('קוד סעיף', '', 0, background_color=BG_COLOR_NAMES)
#             table.set('שם סעיף', '', 1, background_color=BG_COLOR_NAMES)
#             table.set('קוד תחום', '', 10, background_color=BG_COLOR_NAMES)
#             table.set('שם תחום', '', 11, background_color=BG_COLOR_NAMES)
#             table.set('קוד תכנית', '', 20, background_color=BG_COLOR_NAMES)
#             table.set('שם תכנית', '', 21, background_color=BG_COLOR_NAMES)
#             table.set('קוד תקנה', '', 30, background_color=BG_COLOR_NAMES)
#             table.set('שם תקנה', '', 31, background_color=BG_COLOR_NAMES)

#             # print('CCCC', code_titles)
#             if len(code_titles) > 0:
#                 _code, _title, _comment = code_titles.pop(0)
#                 table.set('קוד סעיף', f'="{_code}"', 0, background_color=BG_COLOR_NAMES)
#                 table.set('שם סעיף', _title, 1, background_color=BG_COLOR_NAMES, comment=_comment)
#             if len(code_titles) > 0:
#                 _code, _title, _comment = code_titles.pop(0)
#                 table.set('קוד תחום', f'="{_code}"', 10, background_color=BG_COLOR_NAMES)
#                 table.set('שם תחום', _title, 11, background_color=BG_COLOR_NAMES, comment=_comment)
#             if len(code_titles) > 0:
#                 _code, _title, _comment = code_titles.pop(0)
#                 table.set('קוד תכנית', f'="{_code}"', 20, bold=True, background_color=BG_COLOR_NAMES)
#                 table.set('שם תכנית', _title, 21, bold=True, background_color=BG_COLOR_NAMES, comment=_comment)
#             if len(code_titles) > 0:
#                 _code, _title, _comment = code_titles.pop(0)
#                 table.set('קוד תקנה', f'="{_code}"', 30, background_color=BG_COLOR_NAMES)
#                 table.set('שם תקנה', _title, 31, background_color=BG_COLOR_NAMES, comment=_comment)
#             for _year, _codes in keys:
#                 sum_allocated = None
#                 sum_revised = None
#                 sum_executed = None
#                 titles = []
#                 codes = []
#                 for _code in _codes:
#                     if (_year, _code) in used_keys:
#                         continue

#                     # print(f'Processing inner {_year} {_code}')
#                     if (_year, _code) in raw_map:
#                         raw = raw_map[(_year, _code)]
#                         titles.append(raw['title'])
#                         codes.append(_code)
#                         if raw['net_allocated'] is not None:
#                             sum_allocated = (sum_allocated or 0) + raw['net_allocated']
#                         if raw['net_revised'] is not None:
#                             sum_revised = (sum_revised or 0) + raw['net_revised']
#                         if raw['net_executed'] is not None:
#                             sum_executed = (sum_executed or 0) + raw['net_executed']
#                         used_keys.add((_year, _code))
#                     else:
#                         print(f'Could not find {_year} {_code}')

#                     options = dict(
#                         bold=_year == max_year,
#                         parity=True,
#                         number_format='#,##0.0'
#                     )
#                 codes = ', '.join(codes)
#                 titles = ', '.join(titles)
#                 if codes != code or titles != title:
#                     options['comment'] = f'בשנת {_year} הסעיף נקרא {titles} - {codes}'
#                 if sum_allocated is not None:
#                     if sum_allocated == 0 and _year != max_year:
#                         comment = options.get('comment') or ''
#                         if comment:
#                             comment += '\n'
#                         if sum_revised:
#                             comment += f':התקציב המאושר {sum_revised:,.0f}'
#                         elif sum_executed:
#                             comment += f':התקציב המבוצע {sum_executed:,.0f}'
#                         options_ = dict(options, comment=comment)
#                     else:
#                         options_ = options
#                     table.set(f'{_year}', sum_allocated/1000000, _year*100 + 1, **options_)
#                 if _year == before_proposal_year:
#                     if sum_revised is not None:
#                         table.set(f'{_year} מאושר', sum_revised/1000000, _year*100 + 2, **options)
#                     if sum_executed is not None:
#                         table.set(f'{_year} מבוצע', sum_executed/1000000, _year*100 + 3, **options)
            
#             max_year_allocated = table.get(f'{max_year}')
#             before_proposal_year_allocated = table.get(f'{before_proposal_year}')
#             before_proposal_year_revised = table.get(f'{before_proposal_year} מאושר')
#             if None not in (max_year_allocated, before_proposal_year_allocated) and before_proposal_year_allocated > 0:
#                 change = (max_year_allocated - before_proposal_year_allocated) / before_proposal_year_allocated
#                 change = round(change, 2)
#                 table.set('שינוי מול מקורי 2024', change, (max_year+1)*100 + 1,
#                     bold=False,
#                     parity=1,
#                     background_color=color_scheme_red_green,
#                     number_format='0%'
#                 )
#             if None not in (max_year_allocated, before_proposal_year_revised) and before_proposal_year_revised > 0:
#                 change = (max_year_allocated - before_proposal_year_revised) / before_proposal_year_revised
#                 change = round(change, 2)
#                 table.set('שינוי מול מאושר 2024', change, (max_year+1)*100 + 2,
#                     bold=False,
#                     parity=1,
#                     background_color=color_scheme_red_green,
#                     number_format='0%'
#                 )
    
#     table.save('proposal-compare.xlsx')

# def color_scheme_red_green(value):
#     if not value:
#         return 'FFFFFF'
#     value = float(value)
#     # if value < 0:
#     #     value = min(-value, 1.0)
#     #     value = 0xFF - int(value * (0xFF - 0x88)) 
#     #     # print('FF{:02x}88'.format(value))
#     #     return f'FF{value:02X}{value:02X}'
#     # if value > 0:
#     #     value = min(value, 1.0)
#     #     value = 0xFF - int(value * (0xFF - 0x88))
#     #     # print('88FF{:02x}'.format(value))
#     #     return f'{value:02X}FF{value:02X}'.format(value)
#     if value > .05:
#         return 'FF0000'
#     if value < -.05:
#         return '00FF00'
#     return 'FFFFFF'

def construct_table():
    # process_data()
    outstanding_requests = get_outstanding_requests()
    change_per_year_no_surplus = get_change_per_year_no_surplus()
    original_budget_per_program = get_original_budget_per_program()
    parent_names_per_program = get_parent_names_per_program()
    changes = get_changes(outstanding_requests)
    explanations = get_explanations(outstanding_requests)
    # print('OUTSTANDING REQUESTS:', changes)
    ROW_FIELDS = [
        {
            'title': 'מספר פנייה',
            'key': lambda row: row['committee_id']
        },
        {
            'title': 'קוד סעיף',
            'key': lambda row: row['budget_code'][2:4],
        },
        {
            'title': 'שם סעיף',
            'key': lambda row: parent_names_per_program[row['budget_code'][:4]],
        },
        {
            'title': 'קוד תחום',
            'key': lambda row: row['budget_code'][2:6],
        },
        {
            'title': 'שם תחום',
            'key': lambda row: parent_names_per_program[row['budget_code'][:6]],
        },
        {
            'title': 'קוד תכנית',
            'key': lambda row: row['budget_code'][:8],
        },
        {
            'title': 'שם תכנית',
            'key': lambda row: row['budget_title']
        },
        {
            'title': 'הסבר לתכנית',
            'key': lambda row: explanations.get((row['committee_id'], row['budget_code']), ['', ''])[1],
        },
        {
            'title': 'בקשת השינוי הוצאה נטו במלש"ח',
            'key': lambda row: row['net_expense_diff'] / 1000000,
            'number_format': '#,##0.0',
        },
        {
            'title': 'תיאור בקשה',
            'key': lambda row: row['req_title'],
        },
        {
            'title': 'שם שינוי',
            'key': lambda row: row['change_title'],
        },
        {
            'title': 'מטרת השינוי - מדברי ההסבר',
            'key': lambda row: explanations.get((row['committee_id'], row['budget_code']), ['', ''])[0],
        },
        {
            'title': f'מקורי {YEAR}',
            'key': lambda row: original_budget_per_program.get((YEAR, row['budget_code']), 0) / 1000000,
            'number_format': '#,##0.0',
        },    
        {
            'title': 'שיעור השינוי המבוקש',
            'key': lambda row: (row['net_expense_diff'] / original_budget_per_program.get((YEAR, row['budget_code']), 1)),
            'background_color': color_scheme_red_green,
            'number_format': '0%'
        },
        *sum(
            (
                [
                    {
                        'title': f'מקורי {year}',
                        'key': lambda row, year=year: original_budget_per_program.get((year, row['budget_code']), 0) / 1000000,
                        'number_format': '#,##0.0',
                    },
                    {
                        'title': f'מאושר מנוכה עודפים {year}',
                        'key': lambda row, year=year: (original_budget_per_program.get((year, row['budget_code']), 0) + change_per_year_no_surplus.get((year, row['budget_code']), 0)) / 1000000,
                        'number_format': '#,##0.0',
                    }
                ]
                for year in range(YEAR-1, YEAR - HISTORY_YEARS-1, -1)
            ), start=[]
        )
    ]

    t = Table('שינויים לשנה השוטפת', None, None)
    for row in changes:
        row_key = (row['committee_id'], row['budget_code'])
        t.new_row(row_key)
        for i, field in enumerate(ROW_FIELDS):
            key = field['key']
            value = key(row)
            options = {}
            if field.get('number_format'):
                options['number_format'] = field['number_format']
                options['background_color'] = field.get('background_color')
            else:
                options['background_color'] = BG_COLOR_NAMES
            t.set(field['title'], value, i, **options)
    t.save('budget-changes.xlsx')

if __name__=='__main__':
    construct_table()

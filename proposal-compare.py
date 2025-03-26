import dataflows as DF
from table import Table, BG_COLOR_NAMES, color_scheme_red_green

BUDGET_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/processed/with-extras/datapackage.json'
CONNECTED_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/processed/connected-items-explained/datapackage.json'
MIN_YEAR = 2021

CHECKPOINT_DIR = '.checkpoints/proposal-compare'


def nice_code(code):
    return code[2:]
    # With dots:
    # ret = []
    # code = code[2:]
    # return f'="{code}"'
    # while code:
    #     ret.append(code[:2])
    #     code = code[2:]
    # return '.'.join(ret)

def check_for_active(row):
    if (row['net_allocated'] or row['net_revised'] or row['net_executed']):
        return True
    history = row.get('history') or {}
    for year, rec in history.items():
        year = int(year)
        if year >= MIN_YEAR:
            if rec.get('net_allocated') or rec.get('net_revised') or rec.get('net_executed'):
                return True
    return False

def get_proposal_data():
    raw = DF.Flow(
        DF.load(BUDGET_SOURCE_DATAPACKAGE),
        DF.filter_rows(lambda row: row['year'] >= MIN_YEAR),
        DF.filter_rows(lambda row: not row['code'].startswith('0000')),
        DF.filter_rows(lambda row: not row['code'].startswith('C')),
        DF.filter_rows(lambda row: row['code'] < '0089'),
        DF.filter_rows(lambda row: len(row['code']) >= 4),
        DF.select_fields([
            'code',
            'year',
            'title',
            'non_repeating',
            'net_allocated',
            'net_revised',
            'net_executed',
        ]),
        DF.set_type('code', transform=nice_code),
        DF.checkpoint('raw', CHECKPOINT_DIR),
    ).results()[0][0]
    raw_map = dict(
        ((row['year'], row['code']), row)
        for row in raw
    )
    connected = DF.Flow(
        DF.load(CONNECTED_SOURCE_DATAPACKAGE),
        DF.filter_rows(lambda row: row['year'] >= MIN_YEAR),
        DF.filter_rows(lambda row: not row['code'].startswith('0000')),
        DF.filter_rows(lambda row: not row['code'].startswith('C')),
        DF.filter_rows(lambda row: row['code'] < '0089'),
        DF.filter_rows(lambda row: len(row['code']) >= 4),
        DF.filter_rows(check_for_active),
        # DF.filter_rows(lambda row: (len(row['code']) < 10) or (row['net_allocated'] or row['net_revised'] or row['net_executed'])),
        DF.select_fields([
            'code',
            'year',
            'title',
            'history',
            'hierarchy',
            'is_proposal',
        ]),
        DF.set_type('code', transform=nice_code),
        DF.checkpoint('connected', CHECKPOINT_DIR),
    ).results()[0][0]
    return raw_map, connected

def process_data():
    raw_map, connected = get_proposal_data()
    max_year = max(r['year'] for r in connected)
    proposal_year = list(set(r['year'] for r in connected if r['is_proposal']))
    proposal_year = proposal_year[0] if len(proposal_year) == 1 else None

    if proposal_year is None:
        raise Exception('Could not find a single proposal year, bailing out')
    before_proposal_year = proposal_year - 1

    used_keys = set()
    # table_rows = list()
    # headers = dict()

    titles_for_code_aux = dict()
    for item in raw_map.values():
        if item.get('net_allocated') or item.get('net_revised') or item.get('net_executed'):
            titles_for_code_aux.setdefault(item['code'], dict()).setdefault(item['title'], set()).add(item['year'])
    for k in titles_for_code_aux:
        titles_for_code_aux[k] = sorted((max(y), t) for t, y in titles_for_code_aux[k].items())
    titles_for_code = dict(
        (k, v[-1])
        for k, v in titles_for_code_aux.items()
    )
    titles_comments_for_code = dict()
    for k, v in titles_for_code_aux.items():
        if len(v) > 1:
            titles_comments_for_code[k] = ', '.join(f'עד שנת {y} נקרא {t}' for y, t in v[:-1])
            print('TFC', k, v)

    table = Table('השוואת הצעת התקציב', 
                  group_fields=['קוד סעיף', 'קוד תחום', 'קוד תכנית'],
                  cleanup_fields=['קוד סעיף', 'שם סעיף', 'קוד תחום', 'שם תחום', 'קוד תכנית', 'שם תכנית'])

    for year in range(max_year, MIN_YEAR-1, -1):
        print(f'PROCESSING YEAR {year}, got so far {len(used_keys)} keys')
        for item in connected:
            if item['year'] != year:
                continue
            code = item['code']
            title = item['title']
            # max_year_, title = titles_for_code[code]
            # if item['year'] != max_year:
            #     print(f'Item {code} is not max year {item["year"]} != {max_year}')
            #     continue
            # print(f'Processing {year} {code}')
            key = (year, code)
            if key in used_keys:
                continue
            keys = [(year, [code])]
            history = item['history']
            if history is not None:
                history = list((int(k), v) for k, v in history.items())
                history = sorted(history, key=lambda x: x[0], reverse=True)
                for _year, _rec in history:
                    if _year < MIN_YEAR:
                        break
                    if _rec.get('net_allocated') or _rec.get('net_revised') or _rec.get('net_executed'):
                        _year_codes = [nice_code(x.split(':')[0]) for x in _rec['code_titles']]
                        keys.append((_year, _year_codes))
            # print('KEYS', keys)

            hierarchy = item['hierarchy']
            code_titles = [(nice_code(h[0]), titles_for_code[nice_code(h[0])][1], None) for h in hierarchy[1:]] + [(code, title, titles_comments_for_code.get(code))]

            row_key = (code, year)
            table.new_row(row_key)

            table.set('קוד סעיף', '', 0, background_color=BG_COLOR_NAMES)
            table.set('שם סעיף', '', 1, background_color=BG_COLOR_NAMES)
            table.set('קוד תחום', '', 10, background_color=BG_COLOR_NAMES)
            table.set('שם תחום', '', 11, background_color=BG_COLOR_NAMES)
            table.set('קוד תכנית', '', 20, background_color=BG_COLOR_NAMES)
            table.set('שם תכנית', '', 21, background_color=BG_COLOR_NAMES)
            table.set('קוד תקנה', '', 30, background_color=BG_COLOR_NAMES)
            table.set('שם תקנה', '', 31, background_color=BG_COLOR_NAMES)

            # print('CCCC', code_titles)
            if len(code_titles) > 0:
                _code, _title, _comment = code_titles.pop(0)
                table.set('קוד סעיף', f'="{_code}"', 0, background_color=BG_COLOR_NAMES)
                table.set('שם סעיף', _title, 1, background_color=BG_COLOR_NAMES, comment=_comment)
            if len(code_titles) > 0:
                _code, _title, _comment = code_titles.pop(0)
                table.set('קוד תחום', f'="{_code}"', 10, background_color=BG_COLOR_NAMES)
                table.set('שם תחום', _title, 11, background_color=BG_COLOR_NAMES, comment=_comment)
            if len(code_titles) > 0:
                _code, _title, _comment = code_titles.pop(0)
                table.set('קוד תכנית', f'="{_code}"', 20, bold=True, background_color=BG_COLOR_NAMES)
                table.set('שם תכנית', _title, 21, bold=True, background_color=BG_COLOR_NAMES, comment=_comment)
            if len(code_titles) > 0:
                _code, _title, _comment = code_titles.pop(0)
                table.set('קוד תקנה', f'="{_code}"', 30, background_color=BG_COLOR_NAMES)
                table.set('שם תקנה', _title, 31, background_color=BG_COLOR_NAMES, comment=_comment)
            for _year, _codes in keys:
                sum_allocated = None
                sum_revised = None
                sum_executed = None
                titles = []
                codes = []
                for _code in _codes:
                    if (_year, _code) in used_keys:
                        continue

                    # print(f'Processing inner {_year} {_code}')
                    if (_year, _code) in raw_map:
                        raw = raw_map[(_year, _code)]
                        titles.append(raw['title'])
                        codes.append(_code)
                        if raw['net_allocated'] is not None:
                            sum_allocated = (sum_allocated or 0) + raw['net_allocated']
                        if raw['net_revised'] is not None:
                            sum_revised = (sum_revised or 0) + raw['net_revised']
                        if raw['net_executed'] is not None:
                            sum_executed = (sum_executed or 0) + raw['net_executed']
                        used_keys.add((_year, _code))
                    else:
                        print(f'Could not find {_year} {_code}')

                    options = dict(
                        bold=_year == max_year,
                        parity=True,
                        number_format='#,##0.0'
                    )
                codes = ', '.join(codes)
                titles = ', '.join(titles)
                if codes != code or titles != title:
                    options['comment'] = f'בשנת {_year} הסעיף נקרא {titles} - {codes}'
                if sum_allocated is not None:
                    if sum_allocated == 0 and _year != max_year:
                        comment = options.get('comment') or ''
                        if comment:
                            comment += '\n'
                        if sum_revised:
                            comment += f':התקציב המאושר {sum_revised:,.0f}'
                        elif sum_executed:
                            comment += f':התקציב המבוצע {sum_executed:,.0f}'
                        options_ = dict(options, comment=comment)
                    else:
                        options_ = options
                    table.set(f'{_year}', sum_allocated/1000000, _year*100 + 1, **options_)
                if _year == before_proposal_year:
                    if sum_revised is not None:
                        table.set(f'{_year} מאושר', sum_revised/1000000, _year*100 + 2, **options)
                    if sum_executed is not None:
                        table.set(f'{_year} מבוצע', sum_executed/1000000, _year*100 + 3, **options)
            
            max_year_allocated = table.get(f'{max_year}')
            before_proposal_year_allocated = table.get(f'{before_proposal_year}')
            before_proposal_year_revised = table.get(f'{before_proposal_year} מאושר')
            if None not in (max_year_allocated, before_proposal_year_allocated) and before_proposal_year_allocated > 0:
                change = (max_year_allocated - before_proposal_year_allocated) / before_proposal_year_allocated
                change = round(change, 2)
                table.set('שינוי מול מקורי 2024', change, (max_year+1)*100 + 1,
                    bold=False,
                    parity=1,
                    background_color=color_scheme_red_green,
                    number_format='0%'
                )
            if None not in (max_year_allocated, before_proposal_year_revised) and before_proposal_year_revised > 0:
                change = (max_year_allocated - before_proposal_year_revised) / before_proposal_year_revised
                change = round(change, 2)
                table.set('שינוי מול מאושר 2024', change, (max_year+1)*100 + 2,
                    bold=False,
                    parity=1,
                    background_color=color_scheme_red_green,
                    number_format='0%'
                )
    
    table.save('proposal-compare.xlsx')

if __name__=='__main__':
    process_data()

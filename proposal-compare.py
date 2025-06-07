import dataflows as DF
from table import Table, BG_COLOR_NAMES, color_scheme_red_green

BUDGET_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/processed/with-extras/datapackage.json'
CONNECTED_SOURCE_DATAPACKAGE = 'https://next.obudget.org/datapackages/budget/national/processed/connected-items-explained/datapackage.json'
MIN_YEAR = 2021

CHECKPOINT_DIR = '.checkpoints/proposal-compare'

BASE = dict(font_size=10, font_family='IBM Plex Sans Hebrew')
COLOR_SCHEMES = [
    [
        dict(BASE, color='FFFFFF', background_color='1D5130'),
        dict(BASE, color='FFFFFF', background_color='35AC61'),
        dict(BASE, color='222446', background_color='CBF99F'),
        dict(BASE, color='222446', background_color=['E4FFCB', 'FBFFF3']),
    ],
    [
        dict(BASE, color='FFFFFF', background_color='222446'),
        dict(BASE, color='FFFFFF', background_color='282CC7'),
        dict(BASE, color='222446', background_color='92B8FF'),
        dict(BASE, color='222446', background_color=['D3E2FF','FBFFF3'])
    ],
]
color_scheme = 0

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
            'net_allocated',
            'net_revised',
            'net_executed',
        ]),
        DF.set_type('code', transform=nice_code),
        DF.checkpoint('connected', CHECKPOINT_DIR),
    ).results()[0][0]
    return raw_map, connected

def process_data():
    raw_map, connected = get_proposal_data()
    max_year = max(r['year'] for r in connected)
    all_top_level_codes = sorted(set(r['code'][:2] for r in connected))
    color_scheme_indexes = dict((code, i % 2) for i, code in enumerate(all_top_level_codes))
    # proposal_year = list(set(r['year'] for r in connected if r['is_proposal']))
    # proposal_year = max(r['year'] for r in connected)
    # proposal_year = proposal_year[0] if len(proposal_year) == 1 else None
    # if proposal_year is None:
    #     raise Exception('Could not find a single proposal year, bailing out')
    before_max_year = max_year - 1

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

    histories8 = dict()
    allowed_codes = set()
    for item in connected:        
        year = item['year']
        code = item['code']
        if year < MIN_YEAR:
            continue
        if len(code) < 8:
            continue
        histories8.setdefault(code, []).append((year, item))
    # history_replacement = dict()
    sums = dict()
    for code, items in histories8.items():
        allowed_codes.add(code[:6])
        allowed_codes.add(code[:4])
        allowed_codes.add(code[:2])
        items = sorted(items, key=lambda x: x[0], reverse=True)
        year = items[0][0]
        items = [x[1] for x in items]
        history = items[0]['history']
        for item in items[1:]:
            history[str(item['year'])] = dict(
                net_allocated=item.get('net_allocated'),
                net_revised=item.get('net_revised'),
                net_executed=item.get('net_executed'),
                code_titles=[f"00{item['code']}:{item['title']}"],
            )
            history.update(item['history'])        
        for y, i in [(year, items[0]), *[(int(k), v) for k,v in history.items()]]:
            if y < max_year:
                for l in [2,4,6]:
                    key = y, code[:l]
                    sums.setdefault(key, 0)
                    sums[key] += float(i.get('net_allocated', 0) or 0)
        # history_replacement[(year, code)] = items[0]
            # print(f'REPLACE {year} {code} with {items[0][0]} {items[0][1]["title"]}')
    mismatch = dict()
    for key, value in sums.items():
        actual = raw_map.get(key)
        if not actual:
            continue
        if actual['net_allocated'] != value:
            mismatch[key] = float(actual['net_allocated']) - value

    # return
    table = Table('מעקב תקציב המדינה', 
                  group_fields=['קוד סעיף', 'קוד תחום', 'קוד תכנית'],
                  cleanup_fields=['קוד סעיף', 'שם סעיף', 'קוד תחום', 'שם תחום', 'קוד תכנית', 'שם תכנית'],
                  shrink_columns=['שם סעיף', 'שם תחום', 'שם תכנית'],
            )

    for year in range(max_year, MIN_YEAR-1, -1):
        print(f'PROCESSING YEAR {year}, got so far {len(used_keys)} keys')
        for item in connected:
            if item['year'] != year:
                continue
            code = item['code']
            title = item['title']
            if len(code) < 8 and code not in allowed_codes and year != max_year:
                print(f'Skipping {year} {code} {title}, not allowed')
                continue
            # max_year_, title = titles_for_code[code]
            # if item['year'] != max_year:
            #     print(f'Item {code} is not max year {item["year"]} != {max_year}')
            #     continue
            # print(f'Processing {year} {code}')
            key = (year, code)
            if key in used_keys:
                # print(f'Skipping {year} {code} {title}, already used')
                continue
            keys = [(year, [code])]
            history = item['history']
            if history is not None:
                history = list((int(k), v) for k, v in history.items())
                history = sorted(history, key=lambda x: x[0], reverse=True)
                for _year, _rec in history:
                    if _year < MIN_YEAR:
                        break
                    # if _rec.get('net_allocated') or _rec.get('net_revised') or _rec.get('net_executed'):
                    _year_codes = [nice_code(x.split(':')[0]) for x in _rec['code_titles']]
                    keys.append((_year, _year_codes))
            # print('KEYS', keys)

            hierarchy = item['hierarchy']
            code_titles = [(nice_code(h[0]), titles_for_code[nice_code(h[0])][1], None) for h in hierarchy[1:]] + [(code, title, titles_comments_for_code.get(code))]

            mismatch_comment = dict()
            if (year, code) in mismatch:
                mismatch_comment['comment'] = f'חלק או כלל התקנות תחת סעיף זה מופיעות במיקומן המעודכן נכון לשנת {max_year}'

            # if len(code) < 8:
            #     row_key = (code, year)
            #     table.new_row(row_key)
            # else:
            #     row_key = (code, 2000)
            #     table.new_row(row_key, reuse=True)
            row_key = (code, year)
            table.new_row(row_key)

            schema = COLOR_SCHEMES[color_scheme_indexes[code[:2]]]
            table.set('קוד סעיף', '', 0, **schema[0])
            table.set('שם סעיף', '', 1, **schema[0])
            table.set('קוד תחום', '', 10, **schema[1])
            table.set('שם תחום', '', 11, **schema[1])
            table.set('קוד תכנית', '', 20, **schema[2])
            table.set('שם תכנית', '', 21, **schema[2])
            table.set('קוד תקנה', '', 30, **schema[3])
            table.set('שם תקנה', '', 31, **schema[3])

            values_schema = None
            if len(code_titles) > 0:
                _code, _title, _comment = code_titles.pop(0)
                table.set('קוד סעיף', f'="{_code}"', 0, **schema[0], **(mismatch_comment if len(code_titles) == 0 else {}))
                table.set('שם סעיף', _title, 1, comment=_comment, **schema[0], overflow=True)
                values_schema = schema[0]
                if len(code_titles) > 0:
                    _code, _title, _comment = code_titles.pop(0)
                    table.set('קוד תחום', f'="{_code}"', 10, **schema[1], **(mismatch_comment if len(code_titles) == 0 else {}))
                    table.set('שם תחום', _title, 11, comment=_comment,  **schema[1], overflow=True)
                    values_schema = schema[1]

                    if len(code_titles) > 0:
                        _code, _title, _comment = code_titles.pop(0)
                        table.set('קוד תכנית', f'="{_code}"', 20, bold=True, **schema[2], **(mismatch_comment if len(code_titles) == 0 else {}))
                        table.set('שם תכנית', _title, 21, bold=True, comment=_comment, **schema[2], overflow=True)
                        values_schema = schema[2]

                        if len(code_titles) > 0:
                            _code, _title, _comment = code_titles.pop(0)
                            table.set('קוד תקנה', f'="{_code}"', 30, comment=_comment, **schema[3])
                            table.set('שם תקנה', _title, 31, comment=_comment, **schema[3])
                            values_schema = schema[3]
                        else:
                            table.set('קוד תקנה', '', 30, **schema[2])
                            table.set('שם תקנה', '', 31, **schema[2])
                    else:
                        table.set('קוד תכנית', '', 20, **schema[1])
                        table.set('שם תכנית', '', 21, **schema[1])
                        table.set('קוד תקנה', '', 30, **schema[1])
                        table.set('שם תקנה', '', 31, **schema[1])
                else:
                    table.set('קוד תחום', '', 10, **schema[0])
                    table.set('שם תחום', '', 11, **schema[0])
                    table.set('קוד תכנית', '', 20, **schema[0])
                    table.set('שם תכנית', '', 21, **schema[0])
                    table.set('קוד תקנה', '', 30, **schema[0])
                    table.set('שם תקנה', '', 31, **schema[0])


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
                        number_format='#,##0.0',
                        **values_schema
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
                    table.set(f'{_year} מקורי', sum_allocated/1000000, _year*100 + 1, **options_)
                if _year == before_max_year:
                    if sum_revised is not None:
                        table.set(f'{_year} על״ש', sum_revised/1000000, _year*100 + 2, **options)
                    if sum_executed is not None:
                        table.set(f'{_year} ביצוע', sum_executed/1000000, _year*100 + 3, **options)
                if _year == max_year:
                    if sum_revised is not None and len(code) < 8:
                        table.set(f'{_year} על״ש', sum_revised/1000000, _year*100 + 2, **options)
            
            max_year_allocated = table.get(f'{max_year} מקורי')
            max_year_revised = table.get(f'{max_year} על״ש')
            before_max_year_allocated = table.get(f'{before_max_year} מקורי')
            before_max_year_revised = table.get(f'{before_max_year} על״ש')
            before_max_year_executed = table.get(f'{before_max_year} ביצוע')
            if None not in (before_max_year_revised, before_max_year_allocated) and before_max_year_allocated > 0:
                change = (before_max_year_revised - before_max_year_allocated) / before_max_year_allocated
                change = round(change, 2)
                table.set(f'שיעור שינוי מקורי/על״ש {before_max_year}', change, (max_year+1)*100 + 1,
                    bold=False,
                    parity=1,
                    background_color=color_scheme_red_green(),
                    number_format='0%'
                )
            if None not in (before_max_year_executed, before_max_year_revised) and before_max_year_revised > 0:
                change = before_max_year_executed / before_max_year_revised
                change = round(change, 2)
                table.set(f'שיעור ביצוע מתוך על"ש {before_max_year}', change, (max_year+1)*100 + 2,
                    bold=False,
                    parity=1,
                    background_color='ffffff',
                    number_format='0%'
                )
            if None not in (max_year_allocated, before_max_year_allocated) and before_max_year_allocated > 0:
                change = (max_year_allocated - before_max_year_allocated) / before_max_year_allocated
                change = round(change, 2)
                table.set(f'שיעור שינוי {max_year}/{before_max_year} מקורי', change, (max_year+1)*100 + 3,
                    bold=False,
                    parity=1,
                    background_color=color_scheme_red_green(),
                    number_format='0%'
                )
            if None not in (max_year_allocated, before_max_year_revised) and before_max_year_revised > 0:
                change = (max_year_allocated - before_max_year_revised) / before_max_year_revised
                change = round(change, 2)
                table.set(f'שיעור שינוי {before_max_year} על״ש/{max_year} מקורי', change, (max_year+1)*100 + 4,
                    bold=False,
                    parity=1,
                    background_color=color_scheme_red_green(),
                    number_format='0%'
                )
            if None not in (max_year_revised, max_year_allocated) and max_year_allocated > 0: 
                change = (max_year_revised - max_year_allocated) / max_year_allocated
                change = round(change, 2)
                table.set(f'שיעור שינוי {max_year} מקורי/על״ש', change, (max_year+1)*100 + 5,
                    bold=False,
                    parity=1,
                    background_color=color_scheme_red_green(),
                    number_format='0%'
                )
    
    table.save('proposal-compare.xlsx')

if __name__=='__main__':
    process_data()

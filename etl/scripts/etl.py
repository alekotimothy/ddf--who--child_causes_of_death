# -*- coding: utf-8 -*-

import pandas as pd
import re

source = '../source/childcod_estimates_2000_2017.xls'
readme = open('../source/readme.txt').read()


# manually set mappings
indicator_mapping = dict(
    nnd = 'deaths',
    pnd = 'deaths',
    livebirths = 'live_births',
    f = 'fraction_of_deaths',
    r = 'death_rate'
)

age_mapping = dict(
    neo = 'neo',
    post = 'post',
    ufive = 'u5',
    nnd = 'neo',
    pnd = 'post'
)

mapping = dict(
    neo = "neonatal period",
    post = "postneonatal period",
    # ufive = "under five (i.e., neo+post)",
    ufive = "under five",
    nnd = "total neonatal deaths",
    pnd = "total postneonatal deaths",
    livebirths = 'live births'
)

cats = [("2", "HIV/AIDS"),
        ("3", "Diarrhoeal diseases"),
        ("5", "Tetanus"),
        ("6", "Measles"),
        ("7", "Meningitis/encephalitis"),
        ("8", "Malaria"),
        ("9", "Acute respiratory infections"),
        ("10", "Prematurity"),
        ("11", "Birth asphyxia and birth trauma"),
        ("12", "Sepsis and other infectious conditions of the newborn"),
        ("13", "Other Group 1"),
        ("15", "Congenital anomalies"),
        ("16", "Other noncommunicable diseases"),
        ("17", "Injuries")]

name_regx = re.compile(r'([rf]?)([a-z]+)([0-9]*)')


def create_dimensions(ser_):
    """accepts a series with time/geo index, add cause/age_group dimension to it and return a dataframe"""
    ser = ser_.copy()
    name = ser.name
    c1, c2, c3 = name_regx.match(name).groups()
    if c2 in indicator_mapping:
        indicator = indicator_mapping[c2]
        if c2 in age_mapping:
            age_group = age_mapping[c2]
        else:
            age_group = None
    elif not c1:
        indicator = 'deaths'
        age_group = age_mapping[c2]
    else:
        indicator = indicator_mapping[c1]
        age_group = age_mapping[c2]

    if c3:
        cause = c3
    else:
        cause = None

    index_names = list(ser.index.names)
    ser.name = indicator
    df = ser.reset_index()

    if age_group:
        df['age_group'] = age_group
        index_names.append('age_group')
    if cause:
        df['cause'] = cause
        index_names.append('cause')

    df = df.set_index(index_names)
    return df


def create_datapoints(df_):
    res = {}
    for c in df_.columns:
        ser = df_[c].dropna()
        if not ser.empty:
            df = create_dimensions(ser)
            idx = tuple(df.index.names)
            if idx in res:
                res[idx].append(df)
            else:
                res[idx] = [df]
    res_ = dict([(k, pd.concat(v, sort=True)) for k, v in res.items()])
    for k, v in res_.items():
        by = '--'.join(k)
        for c in v:
            # indicator = v.columns[0]
            v[[c]].dropna().to_csv(f'../../ddf--datapoints--{c}--by--{by}.csv')


def main():
    print('reading source file...')
    data = pd.read_excel(source, sheet_name='estimates')

    for c in cats:
        mapping[c[0]] = c[1]

    print('creating dataset...')

    # datapoints: global
    groups = data.groupby('level')
    gbl = groups.get_group('global').copy()
    gbl['global'] = 'global'
    gbl = gbl.set_index(['global', 'year'])
    gbl_ = gbl.loc[:, 'nnd':]  # datapoints columns begins from `nnd` column

    create_datapoints(gbl_)

    # datapoints: region
    reg = groups.get_group('region').copy()
    reg['region'] = reg['whoreg6'].str.lower()
    reg_ = reg.set_index(['region', 'year']).loc[:, "nnd":]
    create_datapoints(reg_)

    # datapoints: country
    country = groups.get_group('country').copy()
    country['country'] = country['iso3'].str.lower()
    country_ = country.set_index(['country', 'year']).loc[:, 'nnd':]

    # check if index have duplicated (because there are different method to calculate stat for a given year/country)
    assert not country_.index.has_duplicates

    country_ = country.set_index(['country', 'year']).loc[:, 'nnd':'fufive17']
    create_datapoints(country_)

    # entities: geo
    e_gbl = pd.DataFrame({'global': ['global'], 'name': ['global'], 'is--global': ['TRUE']})
    e_reg = reg[['region', 'whoreg6']].dropna().drop_duplicates()

    e_reg.columns = ['region', 'name']
    e_reg['is--region'] = 'TRUE'

    e_country = country[['country', 'whoname', 'iso3', 'whocode', 'whoreg6']].dropna().drop_duplicates(subset='country')
    e_country['region'] = e_country['whoreg6'].str.lower()
    e_country = e_country[['country', 'whoname', 'iso3', 'whocode', 'region']].copy()

    e_country['whocode'] = e_country['whocode'].map(int)
    e_country.columns = ['country', 'name', 'iso3', 'whocode', 'region']
    e_country['is--country'] = "TRUE"

    e_gbl.to_csv('../../ddf--entities--geo--global.csv', index=False)
    e_reg.to_csv('../../ddf--entities--geo--region.csv', index=False)
    e_country.to_csv('../../ddf--entities--geo--country.csv', index=False)

    # entities: age
    e_age = pd.DataFrame([
        ['neo', 'neonatal period'],
        ['post', 'postneonatal period'],
        ['u5', 'under five (i.e., neo+post)']
    ], columns=['age_group', 'name'])

    e_age.to_csv('../../ddf--entities--age_group.csv', index=False)

    # entities: causes
    e_causes = dict(cats)
    e_causes = pd.DataFrame.from_dict(e_causes, orient='index')
    e_causes = e_causes.reset_index()
    e_causes.columns = ['cause', 'name']

    e_causes.to_csv('../../ddf--entities--cause.csv', index=False)

    # concepts
    c_ent = pd.DataFrame([
        ['geo', 'Geo', 'entity_domain', ''],
        ['global', 'Global', 'entity_set', 'geo'],
        ['region', 'World in 6 regions', 'entity_set', 'geo'],
        ['country', 'Country', 'entity_set', 'geo'],
        ['cause', 'Causes', 'entity_domain', ''],
        ['age_group', 'Age Groups', 'entity_domain', '']
    ], columns=['concept', 'name', 'concept_type', 'domain'])

    c_str = pd.DataFrame([
        ['name', 'Name', 'string'],
        ['domain', 'Domain', 'string'],
        ['whocode', 'WHO Code', 'string'],
        ['iso3', 'ISO 3', 'string'],
        ['year', 'Year', 'time']
    ], columns=['concept', 'name', 'concept_type'])

    c_dps = pd.DataFrame([
        ['deaths', 'Total deaths'],
        ['death_rate', 'Death rate'],
        ['fraction_of_deaths', 'fraction of deaths'],
        ['live_births', 'Live Births']
    ], columns=['concept', 'name'])

    c_dps['concept_type'] = 'measure'

    cdf = pd.concat([c_dps, c_ent, c_str], sort=False)
    cdf.to_csv('../../ddf--concepts.csv', index=False)

    print('Done.')


if __name__ == '__main__':
    main()

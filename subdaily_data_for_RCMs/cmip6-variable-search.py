#!/bin/python

import requests
import typing as T
import pandas
import argparse
import logging
import sys

log = logging.getLogger(__name__)

def esgf_api(limit: int = 100, offset: int = 0, facets: T.List[str] = None, fields: T.List[str] = None, replica=False, latest=True, retracted=False, **kwargs):
    """
    Perform a single ESGF API query
    """
    params = {**kwargs, **{
            'format': 'application/solr+json',
            'limit': limit,
            'offset': offset,
            'replica': replica,
        }}

    if facets is not None:
        params['facets'] = ','.join(facets)
    if fields is not None:
        params['fields'] = ','.join(fields)

    r = requests.get('https://esgf.nci.org.au/esg-search/search', params)
    log.debug('GET %s',r.url)

    r.raise_for_status()
    return r.json()


def esgf_api_results_iter(**kwargs):
    """
    Return a stream of results from a ESGF API query, automatically handling pagination
    """
    limit = 1000
    offset = 0

    while True:
        log.debug("Results %d - %d", offset, offset+limit)
        r = esgf_api(limit=limit, offset=offset, **kwargs)

        for d in r['response']['docs']:
            yield {k: v[0] if isinstance(v, list) else v for k, v in d.items() if k != 'score'}

        offset += limit
        if offset > r['response']['numFound']:
            break


def cmip6_match_all_variables(variable_id: T.List[str], fields: T.List[str] = [], **facets):
    """
    Return results where there are matches for *any of* the given variables in a run
    Args:
        facets: See :func:`esgf_api` for details on specifying facets
    Returns:
        Pandas dataframe
    """
    # Group by frequency, in case someone needs variables from multiple realms, but output using the normal esgf id columns
    group_columns = ['mip_era','activity_id','institution_id','source_id','experiment_id','member_id','frequency','grid_label','version']
    index_columns = ['mip_era','activity_id','institution_id','source_id','experiment_id','member_id','table_id','grid_label','version']

    # List of columns to return from ESGF
    fields = set(group_columns + index_columns + fields + ['variable_id', 'instance_id'] + list(facets.keys()))

    # Iterator of results
    r = esgf_api_results_iter(fields=list(fields), variable_id=variable_id, **facets)

    df = pandas.DataFrame.from_records(r)

    # Filter returns True if a group passes the test
    target_values = set(variable_id)
    def filter_match_all(df):
        return True
        actual_values = set(df['variable_id'].unique())

        if target_values != actual_values:
            log.debug('Mismatch %s: %s [%s]', 'variable_id', actual_values, df['instance_id'].values[0])
            return False

        return True

    # Apply the filter and reformat to a multiindex
    df_filtered = df.groupby(group_columns).filter(filter_match_all).sort_values(index_columns + ['variable_id']).set_index(index_columns)

    return df_filtered


def cmip6_facet_argparse(parser):
    group = parser.add_argument_group('search facets', description="ESGF Search Facets, as in the web search. Note substrings will match, e.g. '--var' instead of '--variable_id'")
    group.add_argument('--activity_id', nargs='+')
    group.add_argument('--source_id', nargs='+')
    group.add_argument('--institution_id', nargs='+')
    group.add_argument('--experiment_id', nargs='+')
    group.add_argument('--member_id', nargs='+')
    group.add_argument('--grid_label', nargs='+')
    group.add_argument('--table_id', nargs='+')
    group.add_argument('--frequency', nargs='+')
    group.add_argument('--realm', nargs='+')
    group.add_argument('--variable_id', nargs='+', required=True)
    return parser


def main():
    parser = argparse.ArgumentParser(description="Search ESGF for CMIP6 results that have all the given variables. Output on the terminal is abridged, to see the full list pipe the output to less")
    parser = cmip6_facet_argparse(parser)
    parser.add_argument('--output', type=argparse.FileType('w'), help='save output to this file in CSV format')
    parser.add_argument('--debug', action='store_true', help='print debug info')
    args = vars(parser.parse_args())

    logging.basicConfig()

    output = args.pop('output')
    debug = args.pop('debug')

    if debug:
        log.setLevel(logging.DEBUG)

    # Call the matcher
    df = cmip6_match_all_variables(**args)

    #if not sys.stdout.isatty():
    pandas.set_option('display.max_rows', None)

    # Print just the variable_ids, the full output is in the CSV
    #print(df[['variable_id']])

    if output is not None:
        df.to_csv(output)

if __name__ == '__main__':
    main()

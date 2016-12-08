#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Just a simple wrapper around psql including environment based selection of
credentials, execute from file, automatic json formatting, and jq parsing.

To just enter psql environment with credentials, leave out file and sql args.
However, stdin/out are kind of buggy and I'm giving up on direct psql entrance
via this script. Instead, I've deferred it to a bash wrapper calling this
with the --getcommand flag and then executing
"""

import argparse
from os import environ
from subprocess import run, PIPE, Popen


def get_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-f', '--file', help='file to execute',
                        type=argparse.FileType('r'))
    parser.add_argument('-e', '--environment', nargs='?',
                        choices=['p', 's', 'd'], default='s',
                        help='environment for execution')
    parser.add_argument('-u', '--user', nargs='?', dest='u',
                        help='override environment based user')
    parser.add_argument('-d', '--db', nargs='?', dest='d',
                        help='override environment based db')
    parser.add_argument('-l', '--location', nargs='?', dest='h',
                        help='override environment based location (host)')
    parser.add_argument('-b', '--bash', action='store_true',
                        help='used for hacky bash interop only (used by pgx'
                        'script)')
    parser.add_argument('-v', '--variables', nargs='+',
                        help='list of variables to replace $1 style prepared'
                        'arguments')
    parser.add_argument('--print-sql', action='store_true',
                        help='print the sql itself for debugging')
    parser.add_argument('--print-command', action='store_true',
                        help='print the command itself for debugging')
    parser.add_argument('--no-jq', action='store_true',
                        help='specifically opt out of jq parsing')
    parser.add_argument('--no-json', action='store_true',
                        help='specifically opt out of json return format')
    parser.add_argument('sql', help='sql to execute', nargs='?')
    parser.add_argument('jq', help='jq filter', default='.', nargs='?')
    return parser.parse_args()


def get_env_from_presets(environment):
    if environment == 's':
        return {'h': environ['PG_HOST_EXM'], 'u': environ['PG_USER_LOC'],
                'd': 'exm-staging'}
    if environment == 'd':
        return {'h': 'localhost', 'u': environ['PG_USER_LOC'],
                'd': 'exm-development'}
    return {'h': environ['PG_HOST_EXM'], 'u': environ['PG_USER_ME'],
            'd': 'exm-production'}


def make_replacements(variables, text):
    if variables:
        for i, v in enumerate(variables, 1):
            string_key = '${}'.format(i)
            text = text.replace(string_key, str(v))
    return text


def get_merged_env(environment, **kwargs):
    """ Get final environment variables from all inputs

    Overwrites any environment presets with variables specified via flags like
    --user, if they are present
    """
    return {
        **get_env_from_presets(environment),
        **{k: v for k, v
           in kwargs.items()
           if v is not None
           and k in ('h', 'u', 'd')}}


def format_final_command_and_sql(psql_command, file, sql, variables, no_json,
                                 **kwargs):
    retrieved_sql = (file and file.read()) or sql or None
    replaced_sql = make_replacements(variables, retrieved_sql)
    in_sql_formatter = ('{}' if no_json
                        else 'SELECT array_to_json(array_agg(row_to_json('
                             'sql_to_json_val'
                             '))) FROM ({}) AS sql_to_json_val'
                        ).format(replaced_sql)
    return ('{} -t << EOF\n{}\nEOF'.format(psql_command, in_sql_formatter),
            replaced_sql)


def print_result_with_jq_or_decode(result, sql, file, jq, no_json, no_jq,
                                   **kwargs):
    # Bit of a hack, but if file is provided, then first positional arg is jq
    jq_filter = sql or '.' if file else jq

    if no_json or no_jq:
        return print(result.stdout.decode('utf-8'))

    # Use jq stdout directly
    p = Popen(['jq', jq_filter], stdin=PIPE)
    return p.communicate(input=result.stdout)


def main():
    args = get_args()
    enter_pg_directly = args.file is None and args.sql is None

    # Probably, the invoking script will re-invoke this script without --bash
    # but with the same args otherwise. We're exiting early to say, 'you can
    # use pgx.py as is and not worry about stdin/out oddities'
    if args.bash and not enter_pg_directly:
        return print('continue')

    vars_args = vars(args)
    env_dict = get_merged_env(**vars_args)
    psql_command = 'psql -h {h} -U {u} -d {d}'.format(**env_dict)

    if enter_pg_directly:
        # Bash will want the command to get into the db
        if args.bash:
            return print(psql_command)
        # Use at own risk. This is why all the bash jumping around
        return run([psql_command], shell=True)

    command, replaced_sql = format_final_command_and_sql(psql_command,
                                                         **vars_args)

    if args.print_sql:
        return print(replaced_sql)
    if args.print_command:
        return print(command)

    result = run([command], stdout=PIPE, shell=True)
    return print_result_with_jq_or_decode(result, **vars_args)

main()

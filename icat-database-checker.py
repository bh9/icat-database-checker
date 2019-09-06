#!/usr/bin/env python3

from argparse import ArgumentParser
import json
import psycopg2
import sys
import time

name_checks = {
    'collection': {
        'table': 'r_coll_main',
        'report_columns': ['coll_id', 'coll_name'],
        'name': 'coll_name'},
    'data object': {
        'table': 'r_data_main',
        'report_columns': ['data_id', 'data_name', 'coll_id'],
        'name': 'data_name'},
    'resource': {
        'table': 'r_resc_main',
        'report_columns': ['resc_id', 'resc_name'],
        'name': 'resc_name'},
    'user': {
        'table': 'r_user_main',
        'report_columns': ['user_id', 'user_name'],
        'name': 'user_name'},
    'zone': {
        'table': 'r_zone_main',
        'report_columns': ['zone_id', 'zone_name'],
        'name': 'zone_name'}}

ref_integrity_checks = {
    'collection and data object have same id': {
        'table': 'r_coll_main',
        'report_columns': ['coll_id'],
        'conditions': ['coll_id IN ( SELECT data_id FROM r_data_main)']},
    'parent of collection does not exist': {
        'table': 'r_coll_main',
        'report_columns': ['coll_name'],
        'conditions': ['parent_coll_name NOT IN ( SELECT coll_name from r_coll_main)']},
    'collection of data object does not exist': {
        'table': 'r_data_main',
        'report_columns': [
            'coll_id',
            'data_id',
            'data_name'],
        'conditions': ['coll_id NOT IN ( SELECT coll_id from r_coll_main)']},
    'resource of data object does not exist': {
        'table': 'r_data_main',
        'report_columns': [
            'coll_id',
            'data_id',
            'data_name'],
        'conditions': ['resc_id NOT IN ( SELECT resc_id from r_resc_main)']},
    'object of object access does not exist': {
        'table': 'r_objt_access',
        'report_columns': [
            'object_id',
            'user_id'],
        'conditions': [
            'object_id not in ( SELECT coll_id from r_coll_main)',
            'object_id not in (SELECT data_id from r_data_main)']},
    'user of object access does not exist': {
        'table': 'r_objt_access',
        'report_columns': [
            'object_id',
            'user_id'],
        'conditions': ['user_id not in ( SELECT user_id from r_user_main)']},
    'metamap refers no nonexistent object': {
        'table': 'r_objt_metamap',
        'report_columns': [
            'object_id',
            'meta_id'],
        'conditions': [
            'object_id not in ( SELECT coll_id from r_coll_main)',
            'object_id not in (SELECT data_id from r_data_main)',
            'object_id not in (SELECT user_id from r_user_main)',
            'object_id not in (SELECT resc_id from r_resc_main)']},
    'metamap refers to nonexistent metadata entry': {
        'table': 'r_objt_metamap',
        'report_columns': [
            'object_id',
            'meta_id'],
        'conditions': ['meta_id not in (select meta_id from r_meta_main)']},
    'main quota table refers to nonexistent user': {
        'table': 'r_quota_main',
        'report_columns': [
            'user_id',
            'resc_id'],
        'conditions': ['user_id not in (SELECT user_id from r_user_main)']},
    'main quota table refers to nonexistent resource': {
        'table': 'r_quota_main',
        'report_columns': [
            'user_id',
            'resc_id'],
        'conditions': ['resc_id not in (SELECT resc_id from r_resc_main)']},
    'quota usage table refers to nonexistent user': {
        'table': 'r_quota_usage',
        'report_columns': [
            'user_id',
            'resc_id'],
        'conditions': ['user_id not in (SELECT user_id from r_user_main)']},
    'quota usage table refers to nonexistent resource': {
        'table': 'r_quota_usage',
        'report_columns': [
            'user_id',
            'resc_id'],
        'conditions': ['resc_id not in (SELECT resc_id from r_resc_main)']},
    'resource refers to nonexistent parent resource': {
        'table': 'r_resc_main',
        'report_columns': ['resc_name'],
        'conditions': [
            '( resc_parent = \'\' ) IS FALSE',
            'CAST(resc_parent AS bigint) not in (SELECT resc_id from r_resc_main)']},
    'user refers to nonexistent zone name': {
        'table': 'r_user_main',
        'report_columns': [
            'user_id',
            'zone_name'],
        'conditions': ['zone_name not in (select zone_name from r_zone_main)']},
    'user password table refers to nonexistent user': {
        'table': 'r_user_password',
        'report_columns': ['user_id'],
        'conditions': ['user_id not in (select user_id from r_user_main)']}}

ts_checks = {
    'data object':
    {'table': 'r_data_main',
     'report_columns': ['coll_id', 'data_name', "create_ts", "modify_ts"]},
    'collection object':
    {'table': 'r_coll_main',
     'report_columns': ['coll_id', 'coll_name', "create_ts", "modify_ts"]},
    'object access':
    {'table': 'r_objt_access',
     'report_columns': ['user_id', 'object_id', "create_ts", "modify_ts"]},
    'metadata map':
    {'table': 'r_objt_metamap',
     'report_columns': ['meta_id', 'object_id', "create_ts", "modify_ts"]},
    'resource':
    {'table': 'r_resc_main',
     'report_columns': ['resc_name', "create_ts", "modify_ts"]},
    'rule':
    {'table': 'r_rule_main',
     'report_columns': ['rule_id', "create_ts", "modify_ts"]},
    'zone':
    {'table': 'r_zone_main',
     'report_columns': ['zone_name', "create_ts", "modify_ts"]}
}


def get_arguments():
    desc = 'Performs a number of sanity checks on the iRODS ICAT database'
    parser = ArgumentParser(description=desc)
    parser.add_argument(
        '--config-file',
        help='Location of the irods server_config file (default: etc/irods/server_config.json )',
        default='/etc/irods/server_config.json')
    parser.add_argument(
        '-v',
        action='store_const',
        const=True,
        help='Verbose mode')
    args = parser.parse_args()
    return args


def read_database_config(config_filename):
    with open(config_filename) as configfile:
        data = json.load(configfile)
    return data['plugin_configuration']['database']['postgres']


def get_connection_database(config):
    try:
        connection = psycopg2.connect(user=config['db_username'],
                                      password=config['db_password'],
                                      host=config['db_host'],
                                      port=config['db_port'],
                                      database=config['db_name'])
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to database: ", error)
        sys.exit(1)
    return connection


def check_ref_integrity(connection, table, report_columns, conditions):
    query = "SELECT " + ",".join(report_columns) + " FROM " + \
        table + " WHERE " + " AND ".join(conditions)
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def check_timestamp_order(connection, table, report_columns,
                          first_ts='create_ts', second_ts='modify_ts'):
    query = "SELECT {} FROM {} WHERE CAST ( {} AS INT ) > CAST ( {} AS INT )".format(
        ",".join(report_columns), table, first_ts, second_ts)
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def check_timestamp_future(connection, table, report_columns, max_ts,
                           first_ts='create_ts', second_ts='modify_ts'):
    query = "SELECT {} FROM {} WHERE CAST( {} AS INT) > {} OR CAST( {} AS INT) > {}".format(
        ",".join(report_columns), table, first_ts, max_ts, second_ts, max_ts)
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def check_name_empty(connection, table, name, report_columns):
    query = "SELECT {} FROM {} WHERE {} = ''".format(
        ",".join(report_columns), table, name)
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def check_name_buggy_characters(connection, table, name, report_columns):
    query = r"SELECT {} FROM {} WHERE {} ~ '[\`\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f]'".format(
        ",".join(report_columns), table, name)
    cursor = connection.cursor()
    cursor.execute(query)
    return cursor.fetchall()


def get_collection_name(connection, search_coll_id):
    query = "SELECT coll_name FROM r_coll_main WHERE coll_id = " + search_coll_id
    cursor = connection.cursor()
    cursor.execute(query)
    names = cursor.fetchall()
    if len(names) == 1:
        return names[0][0]
    elif len(names) > 1:
        raise ValueError(
            "Unexpected duplicate result when retrieving collection name")
    else:
        return None
# Main


args = get_arguments()
config = read_database_config(args.config_file)
connection = get_connection_database(config)
issue_found = False

for check_name, check_params in ref_integrity_checks.items():
    if args.v:
        print("Check: referential integrity - " + check_name)
    result = check_ref_integrity(
        connection,
        check_params['table'],
        check_params['report_columns'],
        check_params['conditions'])
    for row in result:
        print("Potential referential integrity issue found: " + check_name)
        column_num = 0
        for report_column in check_params['report_columns']:
            print("  " + str(report_column) + " : " + str(row[column_num]))
            column_num = column_num + 1
        issue_found = True

max_ts = int(time.time()) + 1
for check_name, check_params in ts_checks.items():
    if args.v:
        print("Check: timestamp - " + check_name)

    result_order = check_timestamp_order(
        connection,
        check_params['table'],
        check_params['report_columns'])
    for row in result_order:
        print("Unexpected timestamp order found for " + check_name)
        column_num = 0
        for report_column in check_params['report_columns']:
            print("  " + str(report_column) + " : " + str(row[column_num]))
            column_num = column_num + 1
        issue_found = True

    result_future = check_timestamp_future(
        connection,
        check_params['table'],
        check_params['report_columns'],
        max_ts)
    for row in result_future:
        print("Timestamp in future for " + check_name)
        column_num = 0
        for report_column in check_params['report_columns']:
            print("  " + str(report_column) + " : " + str(row[column_num]))
            column_num = column_num + 1
        issue_found = True

for check_name, check_params in name_checks.items():
    if args.v:
        print("Check: names - " + check_name)

    result_empty = check_name_empty(
        connection,
        check_params['table'],
        check_params['name'],
        check_params['report_columns'])
    for row in result_empty:
        print("Empty name for " + check_name)
        column_num = 0
        for report_column in check_params['report_columns']:
            print("  " + str(report_column) + " : " + str(row[column_num]))
            column_num = column_num + 1
        issue_found = True

    result_buggy_characters = check_name_buggy_characters(
        connection,
        check_params['table'],
        check_params['name'],
        check_params['report_columns'])
    for row in result_buggy_characters:
        print(
            "Name with characters that iRODS processes incorrectly - " +
            check_name)
        column_num = 0
        for report_column in check_params['report_columns']:
            if str(report_column) == 'coll_id':
                coll_name = get_collection_name(
                    connection, str(row[column_num]))
                if coll_name is not None:
                    print("  Collection name : " + coll_name)
            else:
                print("  " + str(report_column) + " : " + str(row[column_num]))
            column_num = column_num + 1
        issue_found = True

sys.exit(2 if issue_found else 0)

import json
import psycopg2


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


def get_collection_name(connection, search_coll_id):
    query = "SELECT coll_name FROM r_coll_main WHERE coll_id = {}".format(
        str(search_coll_id))
    cursor = connection.cursor()
    cursor.execute(query)
    if cursor.rowcount == 1:
        collection = cursor.fetchone()
        return collection[0]
    elif cursor.rowcount > 1:
        raise ValueError(
            "Unexpected duplicate result when retrieving collection name.")
    else:
        return None


def get_dataobject_name(connection, search_data_id):
    query = "SELECT data_name, coll_id FROM r_data_main WHERE data_id = {}".format(
        str(search_data_id))
    cursor = connection.cursor()
    cursor.execute(query)
    
    if cursor.rowcount >= 1:
        # It is possible that we get multiple matches for the dataobject id, because the table
        # has separate entries for replicas
        data_entry = cursor.fetchone()
        return get_collection_name(connection, data_entry[1]) + "/" + data_entry[0]
    else:
        return None


def get_resource_vault_path_dict(connection):
    ''' Returns a dictionary with resource ids (keys) and vault paths (values) of all unixfilesystem resources. '''
    query = "SELECT resc_id, resc_def_path from r_resc_main where resc_type_name = 'unixfilesystem'"
    result = {}
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor:
        result[row[0]] = row[1]
    return result


def get_resource_name_dict(connection):
    '''Returns a dictionary with resource ids (keys) and resource names. '''
    query = "SELECT resc_id, resc_name from r_resc_main"
    result = {}
    cursor = connection.cursor()
    cursor.execute(query)
    for row in cursor:
        result[row[0]] = row[1]
    return result


def get_coll_path_dict(connection):
    '''Returns a dictionary with collection ids (keys) and collection names (values) of all collections. '''
    query = "SELECT coll_id, coll_name FROM r_coll_main"
    result = {}
    cursor = connection.cursor('get_coll_path_dict')
    cursor.execute(query)
    for row in cursor:
        result[row[0]] = row[1]
    return result

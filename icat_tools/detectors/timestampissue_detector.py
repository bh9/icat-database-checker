from icat_tools.detectors.detector import Detector
import psycopg2
import time

class TimestampIssueDetector(Detector):

    def get_name(self):
        return "timestamps"

    def _get_ts_check_data(self):
        data = {
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
        return data.items()

    def _get_prefix_condition(self, table):
        if table == 'r_data_main' and self.args.data_object_prefix is not None:
            return "AND concat ( ( select coll_name from r_coll_main where coll_id = r_data_main.coll_id ), '/', r_data_main.data_name) LIKE '{}%'".format(self.args.data_object_prefix)
        else:
            return ""

    def _check_timestamp_order(self, table, report_columns,
                               first_ts='create_ts', second_ts='modify_ts'):
        query = "SELECT {} FROM {} WHERE CAST ( {} AS INT ) > CAST ( {} AS INT ) {}".format(
            ",".join(report_columns), table, first_ts, second_ts, self._get_prefix_condition(table))
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor

    def _check_timestamp_future(self, table, report_columns, max_ts,
                                first_ts='create_ts', second_ts='modify_ts'):
        query = "SELECT {} FROM {} WHERE CAST( {} AS INT) > {} OR CAST( {} AS INT) > {} {}".format(
            ",".join(report_columns), table, first_ts, max_ts, second_ts, max_ts, self._get_prefix_condition(table))
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor

    def run(self):
        issue_found = False
        max_ts = int(time.time()) + 1
        for check_name, check_params in self._get_ts_check_data():
           if self.args.v:
               self.output_message("Running timestamp order test for: " + check_name)

           result_order = self._check_timestamp_order(
                check_params['table'],
                check_params['report_columns'])
           for row in result_order:
                output = { 'type' : 'order', 'check_name' : check_name, 'report_columns' : {} }
                column_num = 0
                for report_column in check_params['report_columns']:
                    output['report_columns'][str(report_column)] = str(row[column_num])
                    column_num = column_num + 1
                self.output_item(output)
                issue_found = True
           result_order.close()

           if self.args.v:
               self.output_message("Running future timestamp test for: " + check_name)

           result_future = self._check_timestamp_future(
                check_params['table'],
                check_params['report_columns'],
                max_ts)
           for row in result_future:
                output = { 'type' : 'future', 'check_name' : check_name, 'report_columns' : {} }
                column_num = 0
                for report_column in check_params['report_columns']:
                    output['report_columns'][str(report_column)] = str(row[column_num])
                    column_num = column_num + 1
                self.output_item(output)
                issue_found = True
           result_future.close()     

        return issue_found

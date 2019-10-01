from icat_tools import utils
from icat_tools.detectors.detector import Detector
import psycopg2


class RefIntegrityIssueDetector(Detector):
    def get_name(self):
        return "ref_integrity"

    def _get_ref_integrity_data(self):
        data = {
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

        return data.items()

    def _check_ref_integrity(self, table, report_columns, conditions):
        query = "SELECT {} FROM {} WHERE {}".format(
                ",".join(report_columns),
                table,
                " AND ".join(conditions))
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor

    def run(self):
        issue_found = False

        if self.args.data_object_prefix:
            self.output_message("The referential integrity checks do not yet support the --data-object-prefix option.")
            self.output_message("Ignoring this option for these tests.")
        for check_name, check_params in self._get_ref_integrity_data():
            if self.args.v:
                self.output_message("Running referential integrity check for: " + check_name)

            result = self._check_ref_integrity(
                check_params['table'],
                check_params['report_columns'],
                check_params['conditions'])

            for row in result:
                output = {'check_name': check_name, 'report_columns': {}}
                column_num = 0
                for report_column in check_params['report_columns']:
                    output['report_columns'][str(report_column)] = str(
                        row[column_num])
                    column_num = column_num + 1
                self.output_item(output)
                issue_found = True

            result.close()

        return issue_found

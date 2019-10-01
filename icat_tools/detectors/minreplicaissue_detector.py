from icat_tools import utils
from icat_tools.detectors.detector import Detector
import psycopg2


class MinreplicaIssueDetector(Detector):
    def get_name(self):
        return 'minreplicas'

    def run(self):
        issue_found = False
        resource_name_lookup = utils.get_resource_name_dict(self.connection)

        if self.args.data_object_prefix is None:
            query_condition = ""
        else:
            query_condition = "WHERE concat ( ( select coll_name from r_coll_main where coll_id = r_data_main.coll_id ), '/', r_data_main.data_name) LIKE '{}%'".format(self.args.data_object_prefix)

        query = "SELECT data_id, resc_id FROM r_data_main {}".format(query_condition)
        cursor = self.connection.cursor(self.get_name())
        cursor.execute(query)
        data_resc_lookup = {}

        for row in cursor:
            if row[0] in data_resc_lookup:
                if row[1] not in data_resc_lookup[row[0]]:
                    data_resc_lookup[row[0]][row[1]] = ""
            else:
                data_resc_lookup[row[0]] = {row[1]: ""}

        for data_id, resc_dict in data_resc_lookup.items():
            number_replicas = len(resc_dict.keys())
            if number_replicas < self.args.min_replicas:
                issue_found = True
                object_name = utils.get_dataobject_name(
                    self.connection, data_id)
                self.output_item({
                    'object_name': object_name,
                    'number_replicas': number_replicas,
                    'min_replicas': self.args.min_replicas})

        return issue_found

from icat_tools import utils
from icat_tools.detectors.detector import Detector
import psycopg2


class HardlinkDetector(Detector):
    def get_name(self):
        return "hardlinks"

    def run(self):
        issue_found = False
        resource_name_lookup = utils.get_resource_name_dict(self.connection)

        if self.args.data_object_prefix:
            self.output_message("The hard links test does not support the --data-object-prefix option.")
            self.output_message("Ignoring the --data-object-prefix option for this test.")


        for resc_id, resc_path in utils.get_resource_vault_path_dict(
                self.connection).items():

            query = "SELECT data_id, data_path FROM r_data_main WHERE resc_id = {}".format(resc_id)

            lookup_path = {}
            cursor = self.connection.cursor(self.get_name())
            cursor.execute(query)

            for row in cursor:
                if row[1] in lookup_path:
                    issue_found = True
                    this_object = utils.get_dataobject_name(
                        self.connection, row[0])
                    other_object = utils.get_dataobject_name(
                        self.connection, lookup_path[row[1]])
                    if this_object == other_object:
                        self.output_item(
                            {'type': 'duplicate_dataobject_entry',
                             'object_name': this_object,
                             'resource_name': resource_name_lookup[resc_id],
                             'phy_path': row[1]})
                    else:
                        self.output_item(
                            {'type': 'hardlink',
                             'phy_path': row[1],
                             'resource_name': resource_name_lookup[resc_id],
                             'object1': this_object,
                             'object2': other_object})
                else:
                    lookup_path[row[1]] = row[0]

            cursor.close()

        return issue_found

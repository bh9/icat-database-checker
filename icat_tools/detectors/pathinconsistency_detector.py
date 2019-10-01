from icat_tools import utils
from icat_tools.detectors.detector import Detector
import pathlib
import psycopg2


class PathInconsistencyDetector(Detector):
    def get_name(self):
        return "path_consistency"

    def run(self):
        issue_found = False
        resource_path_lookup = utils.get_resource_vault_path_dict(
            self.connection)
        resource_name_lookup = utils.get_resource_name_dict(self.connection)
        coll_path_lookup = utils.get_coll_path_dict(self.connection)

        if self.args.data_object_prefix is None:
            query_condition = ""
        else:
            query_condition = "WHERE concat ( ( select coll_name from r_coll_main where coll_id = r_data_main.coll_id ), '/', r_data_main.data_name) LIKE '{}%'".format(
                        self.args.data_object_prefix)

        query = "SELECT data_id, coll_id, resc_id, data_path FROM r_data_main {}".format(query_condition)
        cursor = self.connection.cursor(self.get_name())
        cursor.execute(query)

        for row in cursor:
            vaultpath = pathlib.Path(resource_path_lookup[row[2]])
            dirname = pathlib.Path(*pathlib.Path(row[3]).parts[:-1])
            dirname_without_vault = dirname.relative_to(vaultpath)
            collname_parts = pathlib.Path(coll_path_lookup[row[1]]).parts
            collname_parts_without_zone = list(collname_parts[2:])
            collname_without_zone = pathlib.Path(*collname_parts_without_zone)
            if collname_without_zone != dirname_without_vault:
                self.output_item({
                    'resource_name': resource_name_lookup[row[2]],
                    'phy_path': row[3],
                    'coll_name': collname_without_zone,
                    'dir_name': dirname_without_vault})
                issue_found = True

        cursor.close()
        return issue_found

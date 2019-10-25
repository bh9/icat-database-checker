from icat_tools import utils
from icat_tools.detectors.detector import Detector
import os
import psycopg2
import re


class MissingIndexDetector(Detector):

    def get_name(self):
        return "indexes"

    def _get_expected_index_filename(self):
        return "/var/lib/irods/packaging/sql/icatSysTables.sql"

    def _get_actual_indexes(self):
        query = "SELECT indexname FROM pg_indexes WHERE schemaname = 'public' ORDER BY indexname"
        cursor = self.connection.cursor("missing_indexes")
        cursor.execute(query)
        return [r[0] for r in list(cursor)]

    def _get_expected_indexes(self):
        results = []
        with open(self._get_expected_index_filename(), 'r') as sqlfile:
            for line in sqlfile:
                index = re.search(
                    r"^create(?:\s+unique)?\s+index\s+(\S+)\s+", line)
                if index is not None:
                    results.append(index.group(1))
        return results

    def run(self):
        issue_found = False

        if not os.path.isfile(self._get_expected_index_filename()):
            self.output_message(
                "Index SQL file not found. Skipping missing index test.")
            return False

        actual_indexes = self._get_actual_indexes()
        for index in self._get_expected_indexes():
            if not (index in actual_indexes):
                self.output_item({
                    'type': 'missing_index', 'index': index})
                issue_found = True

        return issue_found

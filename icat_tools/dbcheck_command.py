from argparse import ArgumentParser, FileType
from enum import Enum
from icat_tools import utils
from icat_tools.dbcheck_outputprocessors import CheckOutputProcessorCSV, CheckOutputProcessorHuman
from icat_tools.detectors.hardlink_detector import HardlinkDetector
from icat_tools.detectors.minreplicaissue_detector import MinreplicaIssueDetector
from icat_tools.detectors.nameissue_detector import NameIssueDetector
from icat_tools.detectors.pathinconsistency_detector import PathInconsistencyDetector
from icat_tools.detectors.refintegrityissue_detector import RefIntegrityIssueDetector
from icat_tools.detectors.timestampissue_detector import TimestampIssueDetector
from icat_tools.detectors.missingindex_detector import MissingIndexDetector
import sys


class TestSubset(Enum):
    ref_integrity = 'ref_integrity'
    timestamps = 'timestamps'
    names = 'names'
    hardlinks = 'hardlinks'
    minreplicas = 'minreplicas'
    path_consistency = 'path_consistency'
    indexes = "indexes"
    all = 'all'

    def __str__(self):
        return self.name

class OutputMode(Enum):
    human = 'human'
    csv = 'csv'

    def __str__(self):
        return self.name

def get_arguments():
    desc = 'Performs a number of sanity checks on the iRODS ICAT database'
    parser = ArgumentParser(description=desc)
    parser.add_argument(
        '--config-file',
        help='Location of the irods server_config file (default: etc/irods/server_config.json )',
        default='/etc/irods/server_config.json')
    parser.add_argument(
        '-m',
        type=OutputMode,
        help='Type of output',
        default='human',
        choices=list(OutputMode))
    parser.add_argument(
        '-v',
        action='store_const',
        const=True,
        help='Verbose mode')
    parser.add_argument(
        '-o','--output',
        type=FileType('w'),
        default=sys.stdout,
        help='Output file (default: standard output)')
    parser.add_argument(
        '--run-test',
        help='Test to run (default: all)',
        default='all',
        type=TestSubset,
        choices=list(TestSubset))
    parser.add_argument(
        '--min-replicas',
        help='Minimum number of replicas that a dataobject must have (default: 1).',
        default=1,
        type=int)
    parser.add_argument(
        '--data-object-prefix',
        help='Only check data objects with a particular prefix. The referential integrity and hard links tests do not support this option yet, and will ignore it. ',
        default=None)
    args = parser.parse_args()
    return args


def main():
    args = get_arguments()
    config = utils.read_database_config(args.config_file)
    connection = utils.get_connection_database(config)

    if args.m.value == 'human':
        output_processor = CheckOutputProcessorHuman(args.output)
    elif args.m.value == 'csv':
        output_processor = CheckOutputProcessorCSV(args.output)
    else:
        print("Error: unknown output processor selected.")
        sys.exit(1)

    detectors = [
        PathInconsistencyDetector(args, connection, output_processor),
        HardlinkDetector(args, connection, output_processor),
        MinreplicaIssueDetector(args, connection, output_processor),
        RefIntegrityIssueDetector(args, connection, output_processor),
        TimestampIssueDetector(args, connection, output_processor),
        NameIssueDetector(args, connection, output_processor),
        MissingIndexDetector(args, connection, output_processor)]

    issue_found = False

    for detector in detectors:
        if args.run_test.value == 'all' or args.run_test.value == detector.get_name():
            if args.v:
                output_processor.output_message("Starting test {}".format(detector.get_name()))
            if detector.run():
                issue_found = True

    if issue_found:
        if args.v:
            output_processor.output_message("Script finished. At least one issue has been detected.")
        sys.exit(2)
    else:
        if args.v:
            output_processor.output_message("Script finished. No issues detected.")
        sys.exit(0)

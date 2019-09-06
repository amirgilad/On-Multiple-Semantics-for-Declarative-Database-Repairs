import os
import sys
import configparser
import getpass

try:
    import readline
except ImportError:
    _readline_available = False
else:
    _readline_available = True

from old.radb import DB
from old.radb import ParsingError
from old.radb.typesys import ValTypeChecker, TypeSysError
from old.radb import ViewCollection
from old import radb as utils
from old.radb import Context, ValidationError, ExecutionError, execute_from_file

import logging
logger = logging.getLogger('ra')


def RATracker(configfile=None, password=False, inputfile=None, outputfile=None,
          echo=False, verbose=False, debug=False, source=configparser.DEFAULTSECT):
    # read system defaults:
    configfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), configfile)
    # print(configfile)
    sys_configfile = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sys.ini')
    sys_config = configparser.ConfigParser()
    if sys_config.read(sys_configfile) == []:
        print('ERROR: required system configuration file {} not found'.format(sys_configfile))
        sys.exit(1)
    defaults = dict(sys_config.items(configparser.DEFAULTSECT))

    if outputfile is not None:
        sys.stdout = utils.Tee(filename=outputfile)
    logging.getLogger('ra').setLevel(logging.DEBUG if debug else
                                     (logging.INFO if verbose else
                                      logging.WARNING))
    logger_handler = logging.StreamHandler(sys.stdout)
    logger_handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logger.addHandler(logger_handler)

    # read user configuration file (starting with system defaults):
    config = configparser.ConfigParser(defaults)
    if config.read(os.path.expanduser(configfile)) == []:
        logger.warning('unable to read configuration file {}; resorting to system defaults' \
                       .format(os.path.expanduser(configfile)))

    # finalize configuration settings, using configuration file and command-line arguments:
    source = configparser.DEFAULTSECT
    if source == configparser.DEFAULTSECT or config.has_section(source):
        configured = dict(config.items(source))
    else:  # args.source is not a section in the config file; treat it as a database name:
        configured = dict(config.items(configparser.DEFAULTSECT))
        configured['db.database'] = source
    if password:
        configured['db.password'] = getpass.getpass('Database password: ')

    # connect to database:
    # print(config, configured)
    if 'db.database' not in configured:
        logger.warning('no database specified')
    try:
        db = DB(configured)
    except Exception as e:
        logger.error('failed to connect to database: {}'.format(e))
        # sys.exit(1)
        return

    # initialize type system:
    try:
        check = ValTypeChecker(configured['default_functions'], configured.get('functions', None))
    except TypeSysError as e:
        logger.error(e)
        # sys.exit(1)
        return

    # construct context (starting with empty view collection):
    context = Context(db, check, ViewCollection())
    try:
        execute_from_file(inputfile, context, echo=echo)
    except (IOError, ParsingError, ValidationError, ExecutionError) as e:
        logger.error(e)
        # sys.exit(1)

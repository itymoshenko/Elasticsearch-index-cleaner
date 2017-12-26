"""
Version 2. Clean old indexes from ES cluster.
Added logging and some improvements.
"""

import elasticsearch
import datetime
import sys
import argparse
import logging

__author__ = 'itymoshenko'

# Test environment (you need to specify the correct IP of ES cluster)
# es = elasticsearch.Elasticsearch([{'host': 'X.X.X.X', 'port': 9200}])

# Prod environment (you need to specify the correct IP of ES cluster)
es = elasticsearch.Elasticsearch([{'host': 'X.X.X.X', 'port': 9200}])

# Logging settings
# log_file = "es_index_cleaner.log"
log_file = "/var/log/elasticsearch/es_index_cleaner.log"
logger = logging.getLogger("Log format")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(log_file)
formatter = logging.Formatter("%(asctime)s:%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def create_parser():
    """ Create parser for input data """
    input_parser = argparse.ArgumentParser()
    input_parser.add_argument('-d', '--days')
    return input_parser


parser = create_parser()
arg = parser.parse_args(sys.argv[1:])
days = int(arg.days)

# Check availability of ElasticSearch
try:
    es.ping()
except elasticsearch.ConnectionError:
    logger.info("Connection to ElasticSearch failed! "
                "Please check it manually!")
    logger.info("Further execution of the script is suspended!")
    logger.info("#" * 72)
    sys.exit(0)


# Get all indexes of ES cluster and store their names into the list
indices = es.indices.get_aliases().keys()
index_list = []
for i in sorted(indices):
    index_list.append(i)


def delete_index(arg):
    """ Delete indexes from ES cluster via API
    :param arg: index_name
    """
    index_name = "{0}".format(arg)
    es.indices.delete(index_name)
    logger.info("Index was deleted successfully")

count = 0


def actions(*args):
    """ Get needed information for every index in list 
    and perform necessary action to it.
    """
    global count
    index_name = "{0}".format(*args)
    settings = es.indices.get_settings(index_name)
    age = settings[index]['settings']['index']['creation_date']
    # check date
    age_parse = datetime.datetime.fromtimestamp(float(age) / 1000.)
    creation_date = age_parse.strftime("%Y-%m-%d")
    delta_time = str(datetime.date.today() - datetime.timedelta(days=days))
    # delete old index
    if creation_date < delta_time and index_name != ".kibana":
        logger.info("Name:{0}. Creation date:{1}.".format(index_name, creation_date))
        delete_index(index_name)
        logger.info("-" * 72)
        count += 1

for index in index_list:
    actions(index)

if count == (len(index_list) - 1):
    logger.info("Total count of deleted indices: {0}".format(count))
elif count > 0:
    logger.info("Other indices is no older than {0} days. "
                "No need to delete them for now!".format(days))
    logger.info("-" * 72)
    logger.info("Total count of deleted indices: {0}".format(count))
else:
    logger.info("Indices older than {0} days have not been found!".format(days))
    logger.info("-" * 72)
    logger.info("Total count of deleted indices: {0}".format(count))

logger.info("#" * 72)

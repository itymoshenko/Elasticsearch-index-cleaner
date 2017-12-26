# Elasticsearch-index-cleaner

This is a simple script that could helps you with deleteing old indices from Elasticsearch cluster.

You need to update your crontab with some new cron job, for example:

0 3 * * * /usr/bin/python /path-to-script-directory/es_index_cleaner.py -d 30

This will delete all indicies olden than 30 days.

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

from socket import create_connection
from django.conf import settings
import json
from graphite.intervals import Interval, IntervalSet
from graphite.logger import log
import time
import random

class BifroestReader(object):
    def __init__(self, metric):
        self.metric = metric

    def get_intervals(self):
        return IntervalSet([Interval(0, time.time())])

    def fetch(self, startTime, endTime):
        for bifroest in ( random.choice( settings.BIFROEST_VALUES ) for i in range( settings.BIFROEST_RETRY_COUNT ) ):
            try:
                s = create_connection(bifroest)
                f = s.makefile()
                f.write(json.dumps({'command':'get_values', 'name':self.metric, 'startTimestamp':startTime, 'endTimestamp':endTime}) + "\n")
                f.flush()
                jsonstring = "\n".join(f.readlines())
                decoded = json.loads(jsonstring)
                return ((decoded['time_def']['start'], decoded['time_def']['end'], decoded['time_def']['step']), decoded['values'])
            except Exception, e:
                log.exception(e)
        return None

class BifroestFinder(object):
    def find_nodes(self, query):
        for bifroest in ( random.choice( settings.BIFROEST_METRICS ) for i in range( settings.BIFROEST_RETRY_COUNT ) ):
            try:
                s = create_connection(bifroest)
                f = s.makefile()
                f.write(json.dumps({'command':'get-sub-metrics', 'query':query.pattern}) + "\n")
                f.flush()
                jsonstring = "\n".join(f.readlines())
                s.close()
                decoded = json.loads(jsonstring)
                for pair in decoded['results']:
                    if pair['isLeaf']:
                        yield LeafNode(pair['path'], BifroestReader(pair['path']))
                    else:
                        yield BranchNode(pair['path'])
            except Exception, e:
                log.exception(e)

    def get_all_nodes(self):
        raise Exception

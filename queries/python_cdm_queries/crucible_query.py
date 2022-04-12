#! /usr/bin/env python3

import json
import sys
import csv
import math
import numpy as np
import pandas as pd
from collections import namedtuple
from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")
print(es.info())
# query = the Elastic DSL, not the Lucene query string

'''
Work in progress document
'''

def run_id_to_tag(main_run_id: str, tag_name: str):
    '''
    Use the run_id to find all the topo tag, return the pair
    '''
    run_id_query = {'match': {'run.id': main_run_id}}
    run_resp =  es.search(
        index="cdmv6dev-tag",
        size = 10_000,
        query = run_id_query,
        source = 'tag'

    )
    #print(json.dumps(run_resp.body,indent=2))
    for tag in run_resp['hits']['hits']:
        if tag['_source']['tag']['name'] == tag_name:
            return tag['_source']['tag']['val'], main_run_id

def run_id_to_all_tags(main_run_id: str):
    '''
    Use the run_id to find all the tags, return the key val pair
    '''
    run_id_query = {'match': {'run.id': main_run_id}}
    run_resp =  es.search(
        index="cdmv6dev-tag",
        size = 10_000,
        query = run_id_query,
        source = 'tag'

    )
    tags=[]
    #print(json.dumps(run_resp.body,indent=2))
    for tag in run_resp['hits']['hits']:
        tags.append((tag['_source']['tag']['name'],tag['_source']['tag']['val']))
    return tags

def tag_to_run_ids(tag_pairs: str):
    '''
    Since tags are linked 1 to a run, a multiple tag query won't work,
    aka I don't know how to say send me all the runIDs with these two tags
    '''
    key,val = tag_pairs.split(':')
    tags_query = {'bool':{'must':[]}}
    tags_query['bool']['must'].append({'match': {'tag.name':{'query': key}}})
    tags_query['bool']['must'].append({'match': {'tag.val':{'query':val}}})
    tag_resp =  es.search(
        index="cdmv6dev-tag",
        size = 10_000,
        #query = {'bool':{'must':[{'match':{'tag.name':{'query':'offload'}}},
            #{'match':{'tag.val':{'query':'False'}}}]}},
        query = tags_query,
        #source = 'run.id'

    )
    results = []
    #print(json.dumps(tag_resp.body,indent=2))
    for document in tag_resp['hits']['hits']:
        results.append(document['_source']['run']['id'])
    return results

def runs_to_tags(runs: list):
    '''
    input: list of run ids
    output: dictionary of {runId:[(tag,val),(tag2,val2)]}
    '''
    #is there a "nice" way to do this in elastic?
    pass

def get_iterations(run_id):
    iteration_query = {'match':{'run.id':run_id}}
    iteration_resp =  es.search(
            index='cdmv6dev-iteration',
            size = 10_000,
            query = iteration_query,
            source = 'iteration.id',
            )
    #print(json.dumps(iteration_resp.body,indent=2))
    iterations = {}
    for iteration in iteration_resp['hits']['hits']:
        #print('\t\t'+iteration['_source']['iteration']['id'])
        iterations[iteration['_source']['iteration']['id']] = {}
    return iterations

    #for item in iteration_resp['hits']['hits']:
        #params_resp =  get_params(str(item['_source']['iteration']['id']))
    #print(params_resp['hits']['hits'])

def get_params(iter_id):
    params_query = {'match':{'iteration.id':iter_id}}
    params_resp =  es.search(
            index='cdmv6dev-param',
            size = 10_000,
            query = params_query,
            )
    #print(json.dumps(params_resp.body, indent=2))
    param_arr=[]
    for param_obj in params_resp['hits']['hits']:
        param_arr.append((param_obj['_source']['param']['arg'],param_obj['_source']['param']['val']))
    #print(param_arr)
    return param_arr

def get_passing_samples(iter_id):
    samples_query = {'match':{'iteration.id':iter_id}}
    samples_resp =  es.search(
            index='cdmv6dev-sample',
            size = 10_000,
            query = samples_query,
            )
    passing_sample_ids = {}
    #print(json.dumps(samples_resp.body,indent=2))
    for result in samples_resp['hits']['hits']:
        if result['_source']['sample']['status'] == 'pass':
            passing_sample_ids.update({result['_source']['sample']['id']: {}})

    return passing_sample_ids

def get_primary_period_id(sample_id):
    period_query = {'match':{'sample.id':sample_id}}
    period_resp = es.search(
            index = 'cdmv6dev-period',
            size = 10_000,
            query = period_query,
            )
    #print(json.dumps(period_resp.body,indent=2))
    periods = {}
    for period in period_resp['hits']['hits']:
        periods.update({'period_id':period['_source']['period']['id'],\
            'begin':period['_source']['period']['begin'],\
            'end': period['_source']['period']['end'],
            'run': period['_source']['run']['id'],
            'source': period['_source']['run']['benchmark'],
            'sample': period['_source']['sample']['num'],
            'type': period['_source']['iteration']['primary-metric']})
    return periods

def get_metric_data_sets(run_id,primary_period,source,type_primary_metric,period_begin,period_end,resolution,metric):
    pass
    # TODO, this function from cdm.js
    begin = int(begin)
    end = int(end)
    resolution = int(resolution)
    duration = math.floor((end - begin) / resolution)
    this_begin = begin
    this_end = begin + duration
    # does the pythonAPI still do ndjson?
    metric_query = {'bool':{'filter':
        [
            {'range': { 'metric_data.end':{'lte':this_end}}},
            {'range': { 'metric_data.begin':{'gte':this_begin}}},
            {'terms': { 'metric_data.id':metric_id}}
        ]#filter
        }
        }
    metric_aggs = {'aggs': {'metric_avg' :
        {'weighted_avg': {'value': {
            'field':'metric_data.value'},
            'weight':{'field':'metric_data.duration'}}}}}

    metric_resp = es.search(
            index = 'metric_data',
            size = 10_000,
            query = metric_query,
            aggs = metric_aggs)
    print(json.dumps(metric_resp.body,indent=2))


def get_metric_desc_id_from_period_id(dict_of_info):
    metric_id_query = {'match':{'period.id':dict_of_info['period_id']}}
    metric_id_resp = es.search(
            index = 'cdmv6dev-metric_desc',
            size = 10_000,
            query = metric_id_query,
            source = 'metric_desc',
            )
    #print(json.dumps(metric_id_resp.body,indent=2))
    for ids in metric_id_resp['hits']['hits']:
        # this matches the primary type, there are multiple metric_ids possible
        if ids['_source']['metric_desc']['type'] == dict_of_info['type']:
            return ids['_source']['metric_desc']['id']

def get_metric_data_from_metric_ids(metric_id):
    metric_data_query = {'term':{'metric_desc.id': metric_id}}
    metric_data_agg = {'agg': { 'weighted_avg':{
        'value':{'field':'metric_data.value'},
        'weight':{'field':'metric_data.duration'}
        }}}

    metric_data_resp = es.search(
            index = 'cdmv6dev-metric_data',
            size = 10_000,
            query = metric_data_query,
            aggs = metric_data_agg
            )
    #print(json.dumps(metric_data_resp.body,indent=2))
    return metric_data_resp['aggregations']['agg']['value']
    #return metric_data_resp.


def main(tag_pair):
    '''
    Need to create a set or some kind of array that will serve as the hierarchy for the results
    use the keys from the tags and the param names to make this set / array. Then use it as the
    names for the indexes. Order matters!
    datapath,topo,test-type,rsize,wsize,proto,pods-per-host,nthreads,sample,metric
    '''


    '''
    want to have {runId:[array of tuples of key val pairs]}
    so I can name the index of the pandas data frame
    '''
    names_for_index = dict()
    run_ids = {k:{} for k in tag_to_run_ids(tag_pair)}
    topos_to_runs = {}
    for run in run_ids:
        run_ids[run].update(run_id_to_all_tags(run))
        for key in run_ids[run].keys():
            if key not in names_for_index:
                names_for_index[key] = []
            else:
                names_for_index[key].append(run_ids[run][key])

    '''
    Map the run_id to a mapping of iterations and params
    iteraionts = {run_id : {iteration_id: { param1: val1, param2: val2,paramN : valN }}}
    iteration_samples = {iteration_id: { sample_id :
                {primary_period: id, begin: msec_unix_epoc, end: msec_unix_epoc},
                {primary_period: id, begin: msec_unix_epoc, end: msec_unix_epoc}}
    '''
    iterations = {}
    iteration_samples = {}
    for run_id in run_ids.keys():
        iterations[run_id] = get_iterations(run_id)
        for iter_id in iterations[run_id]:
            iterations[run_id][iter_id].update(get_params(iter_id))

    # initialize the iteration_samples dict
    for run_id in run_ids:
        for iteration_id in iterations[run_id]:
            iteration_samples[iteration_id] = {}
            #get_metric_id_from_iteration_id(iteration_id)

    for iter_id in iteration_samples:
        iteration_samples[iter_id] = get_passing_samples(iter_id)
        for sample in iteration_samples[iter_id]:
            iteration_samples[iter_id][sample].update(get_primary_period_id(sample))
            # hard code the rest of the kvp in the query for the metricDataSets
            iteration_samples[iter_id][sample]['resolution'] = '1'
            iteration_samples[iter_id][sample]['breakout'] = []
            # with iteration_samples[iter_id][sample] you can call the msearch on the metric_data index

    # Get the actual data for the primary metric, in the primary period, of each sample, of each iteration
    for sample in iteration_samples:
        for period in iteration_samples[sample]:
            a = get_metric_desc_id_from_period_id(iteration_samples[sample][period])
            iteration_samples[sample][period]['primary_metric_desc_id'] = a
            iteration_samples[sample][period]['value'] = float(get_metric_data_from_metric_ids(a))

    print(iteration_samples)
    # TODO group samples by period

    # TODO get a dictWriter CSV module going so you can get this into something usable


    # Now that you have all this, get the actual data. Need primary periods as well
    # print the tags, the params,the sample,  the metric units, then the metric

    #launch a console
    import readline
    import code
    variables = globals().copy()
    variables.update(locals())
    shell = code.InteractiveConsole(variables)
    shell.interact()




if __name__ == '__main__':
    print("this is WIP code, don't use it for important things")
    main(sys.argv[1])

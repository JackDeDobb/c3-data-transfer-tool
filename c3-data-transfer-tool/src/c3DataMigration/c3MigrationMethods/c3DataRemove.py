__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from reprint import output
from c3DataMigration.c3Helpers import c3Request
from c3DataMigration.c3Helpers import c3UsageStats
from c3DataMigration.c3Helpers import c3UtilityMethods





def _startDataRemoveFromEnv (r, p):
  c3TypeToBatchJobMapping = []
  for dataTypeImport in p.dataTypeImports:
    c3Type = dataTypeImport[0]
    if (dataTypeImport[1]['removeData'] != True):
      c3UtilityMethods.printFormatExtraPeriods('Kicking off ' + c3Type, 'REMOVE FLAG IS FALSE', p.maxColumnPrintLength, True)
      continue

    totalRecordsOnEnv = c3UtilityMethods.fetchCountOnType(r, p.errorSleepTimeSeconds, c3Type, '1 == 1')

    url = c3Request.generateTypeActionURL(r, 'AsyncAction', 'submit')
    payload = {
      'spec': {
        'typeName': c3Type,
        'action': 'removeAll',
        'actionName': 'null',
        'args': {
          'allowMultiRowProcessing': (('useSQLOnRemove' in dataTypeImport[1]) and (dataTypeImport[1]['useSQLOnRemove'] == True)),
          'disableAsyncProcessing': (('disableDownstreamOnRemove' in dataTypeImport[1]) and (dataTypeImport[1]['disableDownstreamOnRemove'] == True))
        }
      }
    }
    errorCodePrefix = 'Unsuccessful submitting async removeAll of type ' + c3Type
    request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)
    batchJobId = c3UtilityMethods.createInitialBatchJobStatusEntry(request, c3Type, c3TypeToBatchJobMapping, None)
    c3TypeToBatchJobMapping[-1][1]['status'] = 'running'
    c3TypeToBatchJobMapping[-1][1]['initialFetchCount'] = totalRecordsOnEnv
    c3UtilityMethods.printFormatExtraPeriods('Kicking off ' + c3Type, 'id=' + str(batchJobId), p.maxColumnPrintLength, True)

  return c3TypeToBatchJobMapping




def _finishRemoveDataFromEnv (r, p, c3TypeToBatchJobMapping):
  jobsStillRunning = [x for x in c3TypeToBatchJobMapping if ((x[1]['id'] != None) and (x[1]['completionTime'] == None))]
  with output(output_type='list', initial_len=len(c3TypeToBatchJobMapping), interval=0) as outputLines:
    while (len(jobsStillRunning) > 0):
      time.sleep(p.refreshPollTimeSeconds)

      for c3TypeToBatchJob in jobsStillRunning:
        c3Type = c3TypeToBatchJob[0]

        url = c3Request.generateTypeActionURL(r, 'AsyncAction', 'get')
        payload = {
          'this': {
            'id': c3TypeToBatchJob[1]['id'],
            'actionName': 'null',
          }
        }
        errorCodePrefix = 'Unsuccessful async removeAll status for type ' + c3Type
        request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)

        completed = ET.ElementTree(ET.fromstring(request.text)).getroot().find('./completed').text
        currentFetchCount = c3UtilityMethods.fetchCountOnType(r, p.errorSleepTimeSeconds, c3Type, '1 == 1')

        if (completed == 'true'):
          c3TypeToBatchJob[1]['completionTime'] = datetime.now()
          c3TypeToBatchJob[1]['status'] = 'completed'
        c3TypeToBatchJob[1]['currentFetchCount'] = currentFetchCount

      c3UtilityMethods.printBatchJobStatuses(c3TypeToBatchJobMapping, outputLines, p.maxColumnPrintLength, 'removeAllAsyncAction')
      jobsStillRunning = [x for x in c3TypeToBatchJobMapping if ((x[1]['id'] != None) and (x[1]['completionTime'] == None))]




def _cleanUpAsyncRemoveActions (r, p, c3TypeToBatchJobMapping):
  url = c3Request.generateTypeActionURL(r, 'AsyncAction', 'removeAll')
  payload = {
    'removeFilter': 'intersects(id, [{}])'.format(','.join(['"' + x[1]['id'] + '"' for x in c3TypeToBatchJobMapping])),
    'allowMultiRowProcessing': False,
  }
  errorCodePrefix = 'Unsuccessful cleaning up removeAll AsyncActions'
  c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)




def removeDataFromEnv (r, p):
  if (p.masterRemoveDataSwitch != True):
    return

  c3UtilityMethods.printFormatExtraDashes('REMOVING PREVIOUS DATA FROM THE ENV', p.maxColumnPrintLength, True)
  c3TypeToBatchJobMapping = _startDataRemoveFromEnv(r, p)
  _finishRemoveDataFromEnv(r, p, c3TypeToBatchJobMapping)
  _cleanUpAsyncRemoveActions(r, p, c3TypeToBatchJobMapping)
  c3UsageStats.logAPIRemove(p, c3TypeToBatchJobMapping)

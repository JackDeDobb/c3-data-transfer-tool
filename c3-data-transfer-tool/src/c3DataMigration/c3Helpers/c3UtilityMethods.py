__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import json
import math
import pkg_resources
import requests
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from reprint import output
from c3DataMigration.c3Helpers import c3FileSystem
from c3DataMigration.c3Helpers import c3Request





def stripMetaAndDerivedFieldsFromRecords (records, fieldLabelMap):
  def removeKeyFromObj (entity, field):
    if field in entity:
      del entity[field]

  calcFieldsToRemove = fieldLabelMap['calcFieldArr']
  fKeyFieldsToRemove = fieldLabelMap['foreignKeyFieldArr']
  tvHistoryFieldsToRemove = fieldLabelMap['timedValueHistoryFieldArr']

  for record in records:
    removeKeyFromObj(record, 'meta')
    removeKeyFromObj(record, 'type')
    removeKeyFromObj(record, 'version')
    removeKeyFromObj(record, 'versionEdits')
    [removeKeyFromObj(record, x) for x in calcFieldsToRemove]
    [removeKeyFromObj(record, x) for x in fKeyFieldsToRemove]
    [removeKeyFromObj(record, x) for x in tvHistoryFieldsToRemove]




def printFormatWrapMaxColumnLength (string, maxColumnPrintLength, printToConsole):
  chunks = [string[i:i+maxColumnPrintLength] for i in range(0, len(string), maxColumnPrintLength)]
  if (printToConsole):
    for chunk in chunks:
      print(chunk)
  return chunks




def printFormatExtraPeriods (prefix, suffix, maxColumnPrintLength, printToConsole):
  customStringPeriods = '.' * (maxColumnPrintLength - len(prefix) - len(suffix))
  if (printToConsole):
    print(prefix + customStringPeriods + suffix)

  return [prefix, customStringPeriods, suffix]




def printFormatExtraDashes (printString, maxColumnPrintLength, printToConsole):
  prefix = '_' * math.floor((maxColumnPrintLength - len(printString) - 2) / 2)
  suffix = '_' * math.ceil((maxColumnPrintLength - len(printString) - 2) / 2)

  if (printToConsole):
    print('\n' + prefix + ' ' + printString + ' ' + suffix)

  return [prefix, printString, suffix]




def getLocalVersionC3DataTransferTool ():
  currentVersion = None
  try:
    pkg_resources.get_distribution('c3-data-transfer-tool-jackdedobb').version
  except:
    pass
  return currentVersion




def getLatestVersionC3DataTransferTool ():
  latestVersion = None
  try:
    url = 'https://pypi.org/rss/project/c3-data-transfer-tool-jackdedobb/releases.xml'
    rssFeed = requests.get(url)
    latestVersion = ET.ElementTree(ET.fromstring(rssFeed.text)).getroot().find('./channel/item/title').text
  except:
    pass
  return latestVersion




def getc3Context (r, errorSleepTimeSeconds):
  url = c3Request.generateTypeActionURL(r, 'JS', 'exec')
  payload = {
    'js': 'c3Context()'
  }
  errorCodePrefix = 'Unsuccessful getting c3Context'
  request = c3Request.makeRequest(r, errorSleepTimeSeconds, url, payload, errorCodePrefix)

  c3Context = None
  try:
    c3Context = json.loads(ET.ElementTree(ET.fromstring(request.text)).getroot())
  except:
    try:
      c3Context = json.loads(json.loads(request.text))
    except:
      retVal = c3Request.parseXMLValueFromString(request.text, 'execResponse')
      c3Context = json.loads(retVal)

  return c3Context




def enableQueues (r, p, promptUser=True, listOfQueueNamesToEnable=None):
  if (listOfQueueNamesToEnable == None):
    listOfQueueNamesToEnable = [
      'ActionQueue',
      'BatchQueue',
      'CalcFieldsQueue',
      'MapReduceQueue',
      'ChangeLogQueue',
      'NormalizationQueue',
    ]

  queueNamesToEnable = []
  for queueName in listOfQueueNamesToEnable:
    url = c3Request.generateTypeActionURL(r, queueName, 'isPaused')
    errorCodePrefix = 'Unsuccessful checking status of queue: ' + queueName
    request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, None, errorCodePrefix)
    retVal = c3Request.parseXMLValueFromString(request.text.replace('"', ''), 'isPausedResponse')
    if (retVal == 'true'):
      queueNamesToEnable.append(queueName)

  if ((promptUser == True) and (len(queueNamesToEnable) > 0)):
    string = 'Type (y/yes) to confirm resuming of queues: ' + str(queueNamesToEnable)
    printFormatWrapMaxColumnLength(string, p.maxColumnPrintLength, True)
    if (not (input().lower() in ['y', 'yes'])):
      print('Exiting script.')
      exit(0)

  for queueName in queueNamesToEnable:
    url = c3Request.generateTypeActionURL(r, queueName, 'resume')
    errorCodePrefix = 'Unsuccessful resuming queue: ' + queueName
    request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, None, errorCodePrefix)
    print('Resumed Queue: ' + queueName)




def fetchCountOnType (r, errorSleepTimeSeconds, c3Type, filterString):
  url = c3Request.generateTypeActionURL(r, c3Type, 'fetchCount')
  payload = {
    'spec': {
      'filter': filterString
    }
  }
  errorCodePrefix = 'Unsuccessful fetchCount of type ' + c3Type
  request = c3Request.makeRequest(r, errorSleepTimeSeconds, url, payload, errorCodePrefix)

  retVal = c3Request.parseXMLValueFromString(request.text.replace('"', ''), 'fetchCountResponse')
  return int(retVal)




def retrieveLabeledFields (r, c3Type, errorSleepTimeSeconds):
  jsExecCode = """
    var fieldLabelMap = {
      calcFieldArr: [],
      foreignKeyFieldArr: [],
      timedValueHistoryFieldArr: [],
    };
    INSERT_HERE_FOR_FORMAT.fieldTypes().forEach(function(fieldType) {
      var fieldExtensions = fieldType.extensions().db || {};
      var fieldName = fieldType._init.name;
      if (fieldExtensions.calculated != null) {
        fieldLabelMap.calcFieldArr.push(fieldName);
      }
      if (fieldExtensions.fkey != null) {
        fieldLabelMap.foreignKeyFieldArr.push(fieldName);
      }
      if (fieldExtensions.timedValueHistoryField != null) {
        fieldLabelMap.timedValueHistoryFieldArr.push(fieldName);
      }
    });
    fieldLabelMap
  """.replace('INSERT_HERE_FOR_FORMAT', c3Type)

  url = c3Request.generateTypeActionURL(r, 'JS', 'exec')
  payload = {
    'js': jsExecCode
  }
  errorCodePrefix = 'Unsuccessful getting fieldTypes for: ' + c3Type
  request = c3Request.makeRequest(r, errorSleepTimeSeconds, url, payload, errorCodePrefix)

  fieldTypes = None
  try:
    fieldTypes = json.loads(ET.ElementTree(ET.fromstring(request.text)).getroot())
  except:
    try:
      fieldTypes = json.loads(json.loads(request.text))
    except:
      retVal = c3Request.parseXMLValueFromString(request.text, 'execResponse')
      fieldTypes = json.loads(retVal)

  return fieldTypes




def printBatchJobStatuses (c3TypeToBatchJobMapping, outputLines, maxColumnPrintLength, typeOfBatchJob=None):
  for idx, c3TypeToBatchJob in enumerate(c3TypeToBatchJobMapping):
    c3Type = c3TypeToBatchJob[0]
    batchJobId = c3TypeToBatchJob[1]['id']
    status = c3TypeToBatchJob[1]['status']
    launchTime = c3TypeToBatchJob[1]['launchTime']
    completionTime = c3TypeToBatchJob[1]['completionTime']
    initialFetchCount = c3TypeToBatchJob[1]['initialFetchCount']
    currentFetchCount = c3TypeToBatchJob[1]['currentFetchCount']

    now = datetime.now()
    elapsedTimeString = 'N/A'
    if (batchJobId != None):
      elapsedTime = now - launchTime
      if (completionTime != None):
        elapsedTime = completionTime - launchTime

      days, seconds = elapsedTime.days, elapsedTime.seconds
      hours = days * 24 + seconds // 3600
      minutes = (seconds % 3600) // 60
      seconds = seconds % 60
      elapsedTimeString = 'Elapsed:' + ':'.join([str(hours).zfill(2) + 'h', str(minutes).zfill(2) + 'm', str(seconds).zfill(2) + 's'])

    suffix = None
    prefix = None
    if (typeOfBatchJob == 'removeAllAsyncAction'):
      prefix = 'Removing ' + c3Type
      removeCounts = '{:,}'.format(initialFetchCount - currentFetchCount) + '/' + '{:,}'.format(initialFetchCount)
      suffix = ': '.join([elapsedTimeString, removeCounts, status])
    elif (typeOfBatchJob == 'importAction'):
      prefix = 'Adding ' + c3Type
      removeCounts = '{:,}'.format(currentFetchCount) + '/' + '{:,}'.format(initialFetchCount)
      suffix = ': '.join([elapsedTimeString, removeCounts, status])
    else:
      prefix = 'Checking ' + c3Type
      suffix = ' '.join([elapsedTimeString + ':', status])

    result = printFormatExtraPeriods(prefix, suffix, maxColumnPrintLength, False)
    outputLines[idx] = ''.join(result)




def createInitialBatchJobStatusEntry (request, c3Type, c3TypeToBatchJobMapping, filterString):
  batchJobId = None
  if ((request.text != None) and (request.text != '')):
    try:
      batchJobId = ET.ElementTree(ET.fromstring(request.text)).getroot().find('./id').text
    except:
      batchJobId = json.loads(request.text)['id']
  c3TypeToBatchJobMapping.append([c3Type, {
    'id':                batchJobId,
    'status':            'submitted',
    'launchTime':        datetime.now(),
    'completionTime':    None,
    'initialFetchCount': None,
    'currentFetchCount': None,
    'fileUrls':          [],
    'filter':            filterString, # Only used for Export Data Job
  }])

  return batchJobId




def waitForBatchJobsToComplete (r, p, c3TypeToBatchJobMapping, jobType, typeOfBatchJob=None):
  jobsStillRunning = [x for x in c3TypeToBatchJobMapping if ((x[1]['id'] != None) and (x[1]['status'] in ['submitted', 'running']))]
  with output(output_type='list', initial_len=len(c3TypeToBatchJobMapping), interval=0) as outputLines:
    while (len(jobsStillRunning) > 0):
      time.sleep(p.refreshPollTimeSeconds)
      for c3TypeToBatchJob in jobsStillRunning:
        url = c3Request.generateTypeActionURL(r, jobType, 'get')
        payload = {
          'this': {
            'id':  c3TypeToBatchJob[1]['id']
          },
          'include': 'run',
        }

        errorCodePrefix = 'Unsuccessful grabbing status of ' + jobType + ' for type ' + c3TypeToBatchJob[0]
        request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)

        runStatus = ET.ElementTree(ET.fromstring(request.text)).getroot().find('./run/status/status').text
        c3TypeToBatchJob[1]['status'] = runStatus
        if (runStatus == 'completed'):
          c3TypeToBatchJob[1]['completionTime'] = datetime.now()

        if (typeOfBatchJob == 'importAction'):
          c3TypeToBatchJob[1]['currentFetchCount'] = fetchCountOnType(r, p.errorSleepTimeSeconds, c3TypeToBatchJob[0], '1 == 1')

      printBatchJobStatuses(c3TypeToBatchJobMapping, outputLines, p.maxColumnPrintLength, typeOfBatchJob)
      jobsStillRunning = [x for x in c3TypeToBatchJobMapping if ((x[1]['id'] != None) and (x[1]['status'] in ['submitted', 'running']))]




def outputQueueErrors (r, errorSleepTimeSeconds, dataTypeErrorFileLocation, c3Type, batchJobId):
  url = c3Request.generateTypeActionURL(r, 'InvalidationQueueError', 'fetch')

  payload = {
    'spec': {
      'filter': 'targetObjId == "{}"'.format(batchJobId),
      'limit': 2000
    }
  }
  errorCodePrefix = 'Failed to fetch InvalidationQueue errors for ' + c3Type
  request = c3Request.makeRequest(r, errorSleepTimeSeconds, url, payload, errorCodePrefix)

  with open(dataTypeErrorFileLocation, 'w') as f:
    f.write(request.content.decode('utf-8'))




def outputAllQueueErrorsFromMapping (r, p, c3TypeToBatchJobMapping, jobType):
  queueErrorOutputFolder = '/'.join([p.errorOutputFolder, jobType])
  c3FileSystem.wipeLocalDirectory(p, queueErrorOutputFolder, p.promptUsersForWarnings)

  for c3TypeToBatchJob in c3TypeToBatchJobMapping:
    c3Type = c3TypeToBatchJob[0]
    if (c3TypeToBatchJob[1]['status'] in ['failing', 'failed']):
      dataTypeErrorFileLocation = '/'.join([queueErrorOutputFolder, c3Type + '_errors.xml'])
      outputQueueErrors(r, p.errorSleepTimeSeconds, dataTypeErrorFileLocation, c3Type, c3TypeToBatchJob[1]['id'])
      printFormatExtraPeriods('Generating ' + c3Type, 'DONE', p.maxColumnPrintLength, True)
    else:
      printFormatExtraPeriods('Generating ' + c3Type, 'NO ERRORS', p.maxColumnPrintLength, True)

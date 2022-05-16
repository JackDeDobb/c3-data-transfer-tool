__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import copy
import getpass
import json
import pytz
import requests




def _removeFieldIfExists (obj, field):
  if (field in obj):
    del obj[field]




def _formatDatetimeIfExists (obj, field):
  if (field in obj):
    obj[field] = obj[field].isoformat()




def _omitSensitiveEnvironmentArguments (environmentArguments):
  envArgsDict = vars(copy.deepcopy(environmentArguments))
  _removeFieldIfExists(envArgsDict, 'tenantTag')
  _removeFieldIfExists(envArgsDict, 'userPass')
  _removeFieldIfExists(envArgsDict, 'user')
  _removeFieldIfExists(envArgsDict, 'password')
  _removeFieldIfExists(envArgsDict, 'authToken')

  return envArgsDict




def _cleanC3ToTypeToBatchJobMappingArrays (c3TypeToBatchJobMappingArray):
  array = copy.deepcopy(c3TypeToBatchJobMappingArray)
  for x in array:
    if ('fileUrls' in x[1]):
      x[1]['outputFileCount'] = len(x[1]['fileUrls'])
    _removeFieldIfExists(x[1], 'id')
    _removeFieldIfExists(x[1], 'filter')
    _removeFieldIfExists(x[1], 'fileUrls')
    if ((not ('initialFetchCount' in x[1])) or (x[1]['initialFetchCount'] == None)):
      _removeFieldIfExists(x[1], 'initialFetchCount')
    if ((not ('currentFetchCount' in x[1])) or (x[1]['currentFetchCount'] == None)):
      _removeFieldIfExists(x[1], 'currentFetchCount')
    _formatDatetimeIfExists(x[1], 'launchTime')
    _formatDatetimeIfExists(x[1], 'completionTime')
  
  return array




def _logAPIParmeters (p, stateOfActions, data):
  if (p.sendDeveloperData != True):
    return

  try:
    user = getpass.getuser().replace('.', '-').replace('/', '-')
    initialTimeFormatted = p.initialTime.astimezone(pytz.UTC).strftime("%Y-%m-%dT%H:%M:%S")
    url = '/'.join(['https://c3-data-transfer-tool-default-rtdb.firebaseio.com', user, p.outerAPICall, initialTimeFormatted, stateOfActions]) + '.json'
    requests.put(url, data=json.dumps(data))
  except:
    pass




def logCompletionOfAPI (p):
  try:
    data = {
      'apiStatus': 'COMPLETE'
    }
    _logAPIParmeters(p, 'COMPLETE', data)
  except:
    pass




def logParseArgsAPIParameters (r, p):
  try:
    data = {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
    }
    _logAPIParmeters(p, '01_INITIAL', data)
  except:
    pass




def logCallC3TypeActionAPIParameters (r, p, c3Type, action):
  try:
    data = {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
      'c3Type': c3Type,
      'action': action,
    }
    _logAPIParmeters(p, '01_INITIAL', data)
  except:
    pass




def logUploadAPIParameters(r, p):
  try:
    data = {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
      'dataTypeImports':          p.dataTypeImports,
      'batchSize':                p.batchSize,
      'errorSleepTimeSeconds':    p.errorSleepTimeSeconds,
      'refreshPollTimeSeconds':   p.refreshPollTimeSeconds,
      'maxColumnPrintLength':     p.maxColumnPrintLength,
      'masterRemoveDataSwitch':   p.masterRemoveDataSwitch,
      'masterRefreshDataSwitch':  p.masterRefreshDataSwitch,
      'masterUploadDataSwitch':   p.masterUploadDataSwitch,
    }
    _logAPIParmeters(p, '01_INITIAL', data)
  except:
    pass




def logDownloadAPIParameters (r, p):
  try:
    dataTypeExportsCopy = copy.deepcopy(p.dataTypeExports)
    for x in dataTypeExportsCopy:
      _removeFieldIfExists(x[1], 'filter')

    data = {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
      'dataTypeExports':          dataTypeExportsCopy,
      'errorSleepTimeSeconds':    p.errorSleepTimeSeconds,
      'refreshPollTimeSeconds':   p.refreshPollTimeSeconds,
      'stripMetadataAndDerived':  p.stripMetadataAndDerived,
      'maxColumnPrintLength':     p.maxColumnPrintLength,
      'masterRefreshDataSwitch':  p.masterRefreshDataSwitch,
      'masterDownloadDataSwitch': p.masterDownloadDataSwitch,
    }
    _logAPIParmeters(p, '01_INITIAL', data)
  except:
    pass




def logAPIRefreshCalcs (p, c3TypeToBatchJobMapping):
  try:
    refreshCalcsPrefix = '02' if (p.outerFunctionAPI == 'downloadAPI') else '04'
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)
    for x in c3TypeToBatchJobMappingCopy:
      _removeFieldIfExists(x[1], 'outputFileCount')

    data = {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
    }
    _logAPIParmeters(p, refreshCalcsPrefix + '_REFRESH-CALCS-COMPLETE', data)
  except:
    pass




def logAPIRemove (p, c3TypeToBatchJobMapping):
  try:
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)
    for x in c3TypeToBatchJobMappingCopy:
      _removeFieldIfExists(x[1], 'outputFileCount')
      if (('initialFetchCount' in x[1]) and ('currentFetchCount' in x[1])):
        x[1]['recordsRemoved'] = x[1]['initialFetchCount'] - x[1]['currentFetchCount']
        _removeFieldIfExists(x[1], 'initialFetchCount')
        _removeFieldIfExists(x[1], 'currentFetchCount')

    data = {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
    }
    _logAPIParmeters(p, '02_REMOVE-COMPLETE', data)
  except:
    pass




def logAPIUpload (p):
  try:
    dataTypeImportsCopy = copy.deepcopy(p.dataTypeImports)
    for x in dataTypeImportsCopy:
      _removeFieldIfExists(x[1], 'removeData')
      _removeFieldIfExists(x[1], 'refreshCalcFields')
      _removeFieldIfExists(x[1], 'useSQLOnRemove')
      _removeFieldIfExists(x[1], 'disableDownstreamOnRemove')
      _removeFieldIfExists(x[1], 'files')

    data = {
      'refreshJobMapping': dataTypeImportsCopy,
    }
    _logAPIParmeters(p, '03_UPLOAD-COMPLETE', data)
  except:
    pass




def logAPIUploadZipFiles (p):
  pass




def logAPIUploadCurlFiles (p):
  pass




def logAPIUploadBatchJob (p, c3TypeToBatchJobMapping):
  pass




def logAPIUploadImportErrors (p, c3TypeToBatchJobMapping):
  pass




def logAPIDownloadBatchJob (p, c3TypeToBatchJobMapping):
  try:
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)

    data = {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
    }
    _logAPIParmeters(p, '03_BATCH-JOBS-COMPLETE', data)
  except:
    pass




def logAPIDownloadCurlFiles (p, c3TypeToBatchJobMapping):
  try:
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)

    data = {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
    }
    _logAPIParmeters(p, '04_CURL-FILES-COMPLETE', data)
  except:
    pass




def logAPIDownloadExtractFiles (p, c3TypeToBatchJobMapping):
  try:
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)

    data = {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
    }
    _logAPIParmeters(p, '05_EXTRACT-FILES-COMPLETE', data)
  except:
    pass

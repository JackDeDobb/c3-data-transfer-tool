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




def _omitSensitiveFunctionParameters (functionParameters):
  functionParamsDict = vars(copy.deepcopy(functionParameters))

  _removeFieldIfExists(functionParamsDict, 'errorOutputFolder')
  _removeFieldIfExists(functionParamsDict, 'dataUploadFolder')
  _removeFieldIfExists(functionParamsDict, 'dataDownloadFolder')
  _formatDatetimeIfExists(functionParamsDict, 'initialTime')

  dataTypeExportsCopy = copy.deepcopy(functionParamsDict['dataTypeExports'])
  for x in dataTypeExportsCopy:
    _removeFieldIfExists(x[1], 'filter')
  functionParamsDict['dataTypeExports'] = dataTypeExportsCopy

  return functionParamsDict




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
    print('exception occured')




class parseArgsAPI:
  @staticmethod
  def logStart (r, p):
    state = '01_INITIAL'
    _logAPIParmeters(p, state, {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
      'functionParams': _omitSensitiveFunctionParameters(p),
      'state': state,
    })

  @staticmethod
  def logFinish (p):
    state = 'COMPLETE'
    _logAPIParmeters(p, state, {
      'state': state,
    })




class callC3TypeActionAPI:
  @staticmethod
  def logStart (r, p, c3Type, action):
    state = '01_INITIAL'
    _logAPIParmeters(p, state, {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
      'functionParams': _omitSensitiveFunctionParameters(p),
      'c3Type': c3Type,
      'action': action,
    })

  @staticmethod
  def logFinish (p):
    state = 'COMPLETE'
    _logAPIParmeters(p, 'COMPLETE', {
      'state': state,
    })




class uploadAPI:
  @staticmethod
  def logStart (r, p):
    state = '01_INITIAL'
    _logAPIParmeters(p, state, {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
      'functionParams': _omitSensitiveFunctionParameters(p),
      'state': state,
    })

  def logAPIRemove (p, c3TypeToBatchJobMapping):
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)
    for x in c3TypeToBatchJobMappingCopy:
      _removeFieldIfExists(x[1], 'outputFileCount')
      if (('initialFetchCount' in x[1]) and ('currentFetchCount' in x[1])):
        x[1]['recordsRemoved'] = x[1]['initialFetchCount'] - x[1]['currentFetchCount']
        _removeFieldIfExists(x[1], 'initialFetchCount')
        _removeFieldIfExists(x[1], 'currentFetchCount')

    state = '02_REMOVE-COMPLETE'
    _logAPIParmeters(p, state, {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
      'state': state,
    })

  @staticmethod
  def logZipFiles (p):
    state = '03_ZIP_FILES'
    _logAPIParmeters(p, state, {
      'state': state,
    })

  @staticmethod
  def logCurlFiles (p):
    state = '04_CURL_FILES'
    _logAPIParmeters(p, state, {
      'state': state,
    })

  @staticmethod
  def logBatchJob (p, c3TypeToBatchJobMapping):
    state = '05_BATCH_IMPORT'
    _logAPIParmeters(p, state, {
      'state': state,
    })

  @staticmethod
  def logImportErrors (p, c3TypeToBatchJobMapping):
    state = '06_IMPORT_ERRORS'
    _logAPIParmeters(p, state, {
      'state': state,
    })

  @staticmethod
  def logAPIRefreshCalcs (p, c3TypeToBatchJobMapping):
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)
    for x in c3TypeToBatchJobMappingCopy:
      _removeFieldIfExists(x[1], 'outputFileCount')

    state = '07_REFRESH_CALCS_COMPLETE'
    _logAPIParmeters(p, state, {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
      'state': state,
    })

  @staticmethod
  def logFinish (p):
    state = 'COMPLETE'
    _logAPIParmeters(p, state, {
      'state': state,
    })




class downloadAPI:
  @staticmethod
  def logStart (r, p):
    state = '01_INITIAL'
    _logAPIParmeters(p, state, {
      'environmentArguments': _omitSensitiveEnvironmentArguments(r),
      'functionParams': _omitSensitiveFunctionParameters(p),
      'state': state,
    })

  @staticmethod
  def logAPIRefreshCalcs (p, c3TypeToBatchJobMapping):
    c3TypeToBatchJobMappingCopy = _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping)
    for x in c3TypeToBatchJobMappingCopy:
      _removeFieldIfExists(x[1], 'outputFileCount')

    state = '02_REFRESH_CALCS_COMPLETE'
    _logAPIParmeters(p, state, {
      'refreshJobMapping': c3TypeToBatchJobMappingCopy,
      'state': state,
    })

  @staticmethod
  def logBatchJob (p, c3TypeToBatchJobMapping):
    state = '03_BATCH_JOBS_COMPLETE'
    _logAPIParmeters(p, state, {
      'refreshJobMapping': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
      'state': state,
    })

  @staticmethod
  def logCurlFiles (p, c3TypeToBatchJobMapping):
    state = '04_CURL_FILES_COMPLETE'
    _logAPIParmeters(p, state, {
      'refreshJobMapping': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
      'state': state,
    })

  @staticmethod
  def logExtractFiles (p, c3TypeToBatchJobMapping):
    state = '05_EXTRACT_FILES_COMPLETE'
    _logAPIParmeters(p, state, {
      'refreshJobMapping': _cleanC3ToTypeToBatchJobMappingArrays(c3TypeToBatchJobMapping),
      'state': state,
    })

  @staticmethod
  def logFinish (p):
    state = 'COMPLETE'
    _logAPIParmeters(p, state, {
      'state': state,
    })
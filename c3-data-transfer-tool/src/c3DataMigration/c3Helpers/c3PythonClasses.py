__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
from dataclasses import dataclass, field
from datetime import datetime




@dataclass(frozen=True)
class APIParameters:
  # Common
  errorOutputFolder:        str = ''
  batchSize:                int = 250
  errorSleepTimeSeconds:    int = 15
  refreshPollTimeSeconds:   int = 15
  masterRefreshDataSwitch:  bool = True
  maxColumnPrintLength:     int = 150
  promptUsersForWarnings:   bool = True
  sendDeveloperData:        bool = True
  initialTime:              datetime = datetime.now()
  outerAPICall:             str = ''

  # Upload Specific
  dataTypeImports:          list = field(default_factory=list)
  dataUploadFolder:         str = ''
  masterRemoveDataSwitch:   bool = True
  masterUploadDataSwitch:   bool = True

  # Download Specific
  dataTypeExports:          list = field(default_factory=list)
  dataDownloadFolder:       str = ''
  stripMetadataAndDerived:  bool = True
  masterDownloadDataSwitch: bool = True




  def _validateAndAssignFieldInDict (self, dictionary, field, expectedType, required, defaultValueIfNotRequired=None):
    retBool = True

    if (required):
      retBool = retBool and (field in dictionary)
      retBool = retBool and isinstance(dictionary[field], expectedType)
    else:
      if (field in dictionary):
        retBool = retBool and isinstance(dictionary[field], expectedType)
      else:
        dictionary[field] = defaultValueIfNotRequired

    return retBool




  def _validateDataTypeImports (self):
    dataTypeImports = self.__dict__['dataTypeImports']
    assert(isinstance(dataTypeImports, list))

    for dataTypeImport in dataTypeImports:
      assert(isinstance(dataTypeImport, list) and (len(dataTypeImport) == 2))
      dataType = dataTypeImport[0]
      dataTypeConfig = dataTypeImport[1]

      assert(isinstance(dataType, str))
      assert(isinstance(dataTypeConfig, dict))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'removeData', bool, True))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'uploadData', bool, True))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'refreshCalcFields', bool, True))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'useSQLOnRemove', bool, False, False))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'disableDownstreamOnRemove', bool, False, False))

      dataTypeConfig['gzipFiles'] = []
      dataTypeConfig['remoteFileUrls'] = []




  def _validateDataTypeExports (self):
    dataTypeExports = self.__dict__['dataTypeExports']
    assert(isinstance(dataTypeExports, list))

    for dataTypeExport in dataTypeExports:
      assert(isinstance(dataTypeExport, list) and (len(dataTypeExport) == 2))
      dataType = dataTypeExport[0]
      dataTypeConfig = dataTypeExport[1]

      assert(isinstance(dataType, str))
      assert(isinstance(dataTypeConfig, dict))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'downloadData', bool, True))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'refreshCalcFields', bool, True))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'numRecordsPerFile', int, False, 2000))
      assert(self._validateAndAssignFieldInDict(dataTypeConfig, 'filter', str, False, '1 == 1'))




  def __post_init__ (self):
    d = self.__dict__

    if (d['errorOutputFolder'] == None):
      d['errorOutputFolder'] = (d['dataDownloadFolder'] + '_Errors')

    if (d['maxColumnPrintLength'] == None):
      if (d['outerAPICall'] == 'uploadAPI'):
        d['maxColumnPrintLength'] = min(max([len(x[0]) for x in d['dataTypeImports']]) + 80, 150)
      elif (d['outerAPICall'] == 'downloadAPI'):
        d['maxColumnPrintLength'] = min(max([len(x[0]) for x in d['dataTypeExports']]) + 80, 150)

    self._validateDataTypeImports()
    self._validateDataTypeExports()

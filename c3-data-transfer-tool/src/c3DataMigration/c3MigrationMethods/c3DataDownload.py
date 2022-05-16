__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import xml.etree.ElementTree as ET
from progress.bar import IncrementalBar
from functools import reduce
from c3DataMigration.c3Helpers import c3FileSystem
from c3DataMigration.c3Helpers import c3UsageStats
from c3DataMigration.c3Helpers import c3UtilityMethods
from c3DataMigration.c3Helpers import c3Request






def _startDataDownloadFromEnv(r, p):
  scriptRunnerUsername = c3UtilityMethods.getc3Context(r, p.errorSleepTimeSeconds)['username']

  c3TypeToBatchJobMapping = []
  for dataTypeExport in p.dataTypeExports:
    c3Type = dataTypeExport[0]

    if (dataTypeExport[1]['downloadData'] != True):
      c3UtilityMethods.printFormatExtraPeriods('Kicking off ' + c3Type, 'DOWNLOAD FLAG IS FALSE', p.maxColumnPrintLength, True)
      continue

    recordCount = c3UtilityMethods.fetchCountOnType(r, p.errorSleepTimeSeconds, c3Type, dataTypeExport[1]['filter'])
    dataTypeExport[1]['numFiles'] = round(recordCount / dataTypeExport[1]['numRecordsPerFile'])

    url = c3Request.generateTypeActionURL(r, 'Export', 'startExport')
    payload = {
      'spec': {
        'targetType':                 c3Type,
        'contentType':                'json',
        'jsonInclude':                'this',
        'filter':                     dataTypeExport[1]['filter'],
        'fileUrlOrEncodedPathPrefix': '/'.join(['c3-cp/exports', scriptRunnerUsername, c3Type]),
        'failIfUrlNotEmpty':          False,
        'contentEncoding':            'gzip',
        'numFiles':                   dataTypeExport[1]['numFiles'],
      }
    }
    errorCodePrefix = 'Unsuccessful kicking off export of type ' + c3Type
    request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)

    batchJobId = c3UtilityMethods.createInitialBatchJobStatusEntry(request, c3Type, c3TypeToBatchJobMapping, dataTypeExport[1]['filter'])
    c3UtilityMethods.printFormatExtraPeriods('Kicking off ' + c3Type, 'id=' + str(batchJobId), p.maxColumnPrintLength, True)

  return c3TypeToBatchJobMapping




def _finishDataDownloadFromEnv(r, p, c3TypeToBatchJobMapping):
  jobType = 'Export'
  c3UtilityMethods.waitForBatchJobsToComplete(r, p, c3TypeToBatchJobMapping, jobType, 'exportAction')

  for c3TypeToBatchJob in c3TypeToBatchJobMapping:
    if (c3TypeToBatchJob[1]['status'] in ['completed']):
      url = c3Request.generateTypeActionURL(r, 'Export', 'files')
      payload = {
        'this': {
          'id': c3TypeToBatchJob[1]['id']
        }
      }
      errorCodePrefix = 'Unsuccessful retrieving files for export of type ' + c3TypeToBatchJob[0]
      request = c3Request.makeRequest(r, p.errorSleepTimeSeconds, url, payload, errorCodePrefix)

      fileUrls = [x.text for x in ET.ElementTree(ET.fromstring(request.text)).getroot().findall('./filesResponse/v/url')]
      c3TypeToBatchJob[1]['fileUrls'] = fileUrls




def _fetchGeneratedExportFiles (r, p, c3TypeToBatchJobMapping):
  c3FileSystem.wipeLocalDirectory(p.dataDownloadFolder)
  for c3TypeToBatchJob in c3TypeToBatchJobMapping:
    c3Type = c3TypeToBatchJob[0]
    fileUrls = c3TypeToBatchJob[1]['fileUrls']
    dataTypeFilesFolderPath = '/'.join([p.dataDownloadFolder, c3Type])
    c3FileSystem.wipeLocalDirectory(dataTypeFilesFolderPath)

    if (c3TypeToBatchJob[1]['status'] in ['completed']):
      if (len(fileUrls) == 0):
        c3UtilityMethods.printFormatExtraPeriods('Fetching ' + c3Type, 'NO EXPORT FILES', p.maxColumnPrintLength, True)
        continue

      typeFetchCountWithFilter = c3UtilityMethods.fetchCountOnType(r, p.errorSleepTimeSeconds, c3Type, c3TypeToBatchJob[1]['filter'])
      okayToSkip404Error = (typeFetchCountWithFilter == 0)

      result = c3UtilityMethods.printFormatExtraPeriods('Fetching ' + c3Type, ' |████████████████████████████████|', p.maxColumnPrintLength, False)
      progressBar = IncrementalBar(''.join(result[:2]), max=len(fileUrls))
      for idx, fileUrl in enumerate(fileUrls):
        downloadFilePath = '/'.join([dataTypeFilesFolderPath, str(idx) + '.json.gz'])
        fullFileURL = c3Request.generateFileURL(r, fileUrl)
        errorCodePrefix = 'Unsuccessful pulling ' + c3Type + ': ' + fullFileURL
        c3Request.downloadFileFromURL(r, p.errorSleepTimeSeconds, fullFileURL, downloadFilePath, okayToSkip404Error, errorCodePrefix)
        [progressBar.next() for _ in range(1)]
      progressBar.finish()
    else:
      c3UtilityMethods.printFormatExtraPeriods('Fetching ' + c3Type, 'EXPORT JOB FAILED', p.maxColumnPrintLength, True)




def _extractGeneratedExportFiles (r, p, c3TypeToBatchJobMapping):
  c3Types = [x[0] for x in c3TypeToBatchJobMapping]
  c3FileSystem.unzipFilesInDirectory(r, p, p.dataDownloadFolder, c3Types)




def _cleanUpGeneratedExportFiles(r, p, c3TypeToBatchJobMapping):
  listOfListOfFileUrls = [x[1]['fileUrls'] for x in c3TypeToBatchJobMapping]
  flattenedListFileUrls = reduce(lambda z, y : z + y, listOfListOfFileUrls)
  c3FileSystem.deleteRemoteFiles(r, p, flattenedListFileUrls)




def downloadDataFromEnv (r, p):
  if (p.masterDownloadDataSwitch != True):
    return

  c3UtilityMethods.printFormatExtraDashes('DOWNLOADING DATA FROM THE ENV', p.maxColumnPrintLength, True)
  c3TypeToBatchJobMapping = _startDataDownloadFromEnv(r, p)
  _finishDataDownloadFromEnv(r, p, c3TypeToBatchJobMapping)
  c3UsageStats.logAPIDownloadBatchJob(p, c3TypeToBatchJobMapping)

  c3UtilityMethods.printFormatExtraDashes('CURLING DOWN GENERATED EXPORT FILES', p.maxColumnPrintLength, True)
  _fetchGeneratedExportFiles(r, p, c3TypeToBatchJobMapping)
  c3UsageStats.logAPIDownloadCurlFiles(p, c3TypeToBatchJobMapping)

  c3UtilityMethods.printFormatExtraDashes('EXTRACTING GENERATED EXPORT FILES', p.maxColumnPrintLength, True)
  _extractGeneratedExportFiles(r, p, c3TypeToBatchJobMapping)
  _cleanUpGeneratedExportFiles(r, p, c3TypeToBatchJobMapping)
  c3UsageStats.logAPIDownloadExtractFiles(p, c3TypeToBatchJobMapping)

  c3UtilityMethods.printFormatExtraDashes('SCANNING DOWNLOAD FOLDER INFO', p.maxColumnPrintLength, True)
  c3FileSystem.scanFilesInDirectory(p, p.dataTypeExports, p.dataDownloadFolder, True)

  c3UtilityMethods.printFormatExtraDashes('GENERATING EXPORT QUEUE ERROR FILES', p.maxColumnPrintLength, True)
  c3UtilityMethods.outputAllQueueErrorsFromMapping(r, p.errorSleepTimeSeconds, p.maxColumnPrintLength, p.errorOutputFolder, c3TypeToBatchJobMapping, 'Export')

__author__ = 'Jackson DeDobbelaere'
__credits__ = ['Jackson DeDobbelaere']
__maintainer__ = 'Jackson DeDobbealere'
__email__ = 'jackson.dedobbelaere@c3.ai'


#!/usr/bin/env python3
import json
import requests
import time




def generateTypeActionURL (r, c3Type, action):
  return '/'.join([r.env, 'api', '1', r.tenant, r.tag, c3Type]) + '?action=' + action




def generateFileURL (r, c3FileSystemPath):
  return '/'.join([r.env, 'file', '1', r.tenant, r.tag, c3FileSystemPath])




def parseXMLValueFromString (xmlString, label):
  retString = xmlString
  retString = retString.replace('<' + label + ' version="2.0">', '')
  retString = retString.replace('<' + label + ' version=2.0>', '')
  retString = retString.replace('</' + label + '>', '')

  return retString




lastRefreshTime = 0
def _refreshAuthToken (r):
  global lastRefreshTime
  if ((time.time() - lastRefreshTime) > (5 * 60 * 1000)): # 5 minute refresh time
    url = generateTypeActionURL(r, 'Authenticator', 'generateC3AuthToken')
    retVal = None
    if (r.authToken):
      headers = { 'Content-type': 'application/json', 'Authorization': r.authToken }
      retVal = requests.post(url=url, headers=headers)
    elif (r.user and r.password):
      headers = { 'Content-type': 'application/json', 'Accept': 'application/json' }
      retVal = requests.post(url=url, headers=headers, auth=(r.user, r.password))
    r.authToken = parseXMLValueFromString(retVal.text.replace('"', ''), 'generateC3AuthTokenResponse')
    lastRefreshTime = time.time()




def _makeRequestHelper (r, url, payload):
  retVal = None
  if (r.authToken):
    headers = { 'Content-type': 'application/json', 'Authorization': r.authToken }
    if (payload == None):
      retVal = requests.post(url=url, headers=headers)
    else:
      retVal = requests.post(url=url, data=json.dumps(payload), headers=headers)
  elif (r.user and r.password):
    headers = { 'Content-type': 'application/json', 'Accept': 'application/json' }
    if (payload == None):
      retVal = requests.post(url=url, headers=headers, auth=(r.user, r.password))
    else:
      retVal = requests.post(url=url, json=payload, headers=headers, auth=(r.user, r.password))
  _refreshAuthToken(r)

  return retVal




def makeRequest (r, errorSleepTimeSeconds, url, payload, errorCodePrefix):
  request = _makeRequestHelper(r, url, payload)
  while (request.status_code != 200):
    print(errorCodePrefix + ' w/ status code: ' + str(request.status_code))
    print('Error Message: ' + request.text)
    print('Sleeping ' + str(errorSleepTimeSeconds) + ' seconds, and retrying. Use Control-C to kill program.')
    time.sleep(errorSleepTimeSeconds)
    request = _makeRequestHelper(r, url, payload)

  return request




def downloadFileFromURL (r, errorSleepTimeSeconds, fullFileURL, downloadFilePath, okayToSkip404Error, errorCodePrefix):
  def createFileRequest ():
    _refreshAuthToken(r)
    cookies = {
      'c3auth': r.authToken
    }
    return requests.get(fullFileURL, stream=True, cookies=cookies)

  shouldStreamFile = True
  fileRequest = createFileRequest()
  while (fileRequest.status_code != 200):
    if ((fileRequest.status_code == 404) and (okayToSkip404Error == True)):
      shouldStreamFile = False
      break
    print(errorCodePrefix + ' w/ status code: ' + str(fileRequest.status_code))
    print('Error Message: ' + fileRequest.text)
    print('Sleeping ' + str(errorSleepTimeSeconds) + ' seconds, and retrying. Use Control-C to kill program.')
    time.sleep(errorSleepTimeSeconds)
    fileRequest = createFileRequest()

  if (shouldStreamFile == True):
    with open(downloadFilePath, 'wb') as f:
      for chunk in fileRequest.iter_content(chunk_size=8192):
        f.write(chunk)

  return downloadFilePath




def uploadFileToURL (r, errorSleepTimeSeconds, fullFileURL, uploadFilePath, errorCodePrefix):
  def createFileRequest (file):
    _refreshAuthToken(r)
    cookies = {
      'c3auth': r.authToken
    }
    return requests.post(fullFileURL, data=file, stream=True, cookies=cookies)

  file = open(uploadFilePath, 'rb')
  fileRequest = createFileRequest(file)
  while (fileRequest.status_code != 200):
    print(errorCodePrefix + ' w/ status code: ' + str(fileRequest.status_code))
    print('Error Message: ' + fileRequest.text)
    print('Sleeping ' + str(errorSleepTimeSeconds) + ' seconds, and retrying. Use Control-C to kill program.')
    time.sleep(errorSleepTimeSeconds)
    fileRequest = createFileRequest(file)

  return uploadFilePath




# TODO: Upload streaming file in case of large files (below code should replace uploadFileToURL)
# import os

# def _readInChunks(file_object, chunkSize):
#   while True:
#     data = file_object.read(chunkSize)
#     if not data:
#       break
#     yield data


# def upload (file, url):
#   content_name = str(file)
#   content_path = os.path.abspath(file)
#   content_size = os.stat(content_path).st_size
#   print(content_name, content_path, content_size)

#   file_object = open(content_path, 'rb')
#   index = 0
#   offset = 0
#   headers = {}

#   for chunk in _readInChunks(file_object, 8192):
#     offset = index + len(chunk)
#     headers['Content-Range'] = 'bytes %s-%s/%s' % (index, offset - 1, content_size)
#     headers['Authorization'] = auth_string
#     index = offset
#     try:
#       file = {"file": chunk}
#       r = requests.post(url, files=file, headers=headers)
#       print(r.json())
#       print("r: %s, Content-Range: %s" % (r, headers['Content-Range']))
#     except Exception as e:
#       print(e)

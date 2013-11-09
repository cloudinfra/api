import requests
import json

class colors:
    GREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

API_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
API_SECRET = "YYYYYYYYYYYYYYYYYYYYYYY"

S3KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
S3PASS = "YYYYYYYYYYYYYYYYYYYYYYY"


EMAIL = "xyz@abc.com"
URL = "http://cloudinfra.in/api/v1"

def perform_authenticate():
  auth_uri = URL + "/authenticate"
  auth_params = {"apiKey" : API_KEY, "apiSecret" : API_SECRET, "email" : EMAIL}
  res = requests.post(auth_uri, auth_params)
  response_hash = json.loads(res.text)
  auth_code = response_hash["authCode"]
  return auth_code

def post(url, params):
  print colors.GREEN + "Requesting : " + url + "  with params : " + str(params) + "\n" + colors.ENDC
  res = requests.post(url, data = params, timeout = 10 * 60)
  print res.text
  response_hash = json.loads(res.text)
  if "error" in response_hash.keys():
    print colors.FAIL + "Error : " + response_hash["error"] + colors.ENDC
    return None
  return response_hash["result"]


def perform_ls(auth_code, email, filepattern):
  ls_params = {"authCode" : auth_code, "email" : email, "filepattern" : filepattern}
  ls_uri = URL + "/ls"
  result = post(ls_uri, ls_params)
  if result is not None:
    print result
  return None

def perform_store(auth_code, email, s3location, localfilename, credentialname):
  store_params = {"authCode" : auth_code, "email" : email, "s3location" : s3location, "localfilename" : localfilename, "credentialname" : credentialname}
  store_uri = URL + "/store"
  result = post(store_uri, store_params)
  return result

def perform_add_credential(auth_code, email, cloud_type, name, cloud_key, cloud_secret):
  add_params = {"authCode" : auth_code, "email" : EMAIL, "cloudType" : cloud_type, "name" : name, "cloudSecret" : cloud_secret, "cloudKey" : cloud_key}
  add_uri = URL + "/addCloudCredential"
  result = post(add_uri, add_params)
  return result


def perform_list_credentials(auth_code, email):
  list_params = {"authCode" : auth_code, "email" : email}
  list_uri = URL + "/listCloudCredentials"
  result = post(list_uri, list_params)
  return result

def perform_load(auth_code, email, s3location, localfilename, credentialname):
  load_params = {"authCode" : auth_code, "email" : email, "s3location" : s3location, "localfilename" : localfilename, "credentialname" : credentialname}
  load_uri = URL + "/load"
  result = post(load_uri, load_params)
  return result


def perform_view(auth_code, email, distpipeline, centralpipeline, distsizelimit, centralsizelimit):
  view_params = {"authCode" : auth_code, "email" : email, "distpipeline" : distpipeline, "centralpipeline" : centralpipeline, "centralsizelimit" : centralsizelimit, "distsizelimit" : distsizelimit}
  view_uri = URL + "/view"
  result = post(view_uri, view_params)
  return result


def perform_transform(auth_code, email, mappipeline, localfilename):
  transform_params = {"authCode" : auth_code, "email" : EMAIL, "mappipeline" : mappipeline, "localfilename" : localfilename}
  transform_uri = URL + "/transform"
  result = post(transform_uri, transform_params)
  return result



def perform_delete_credential(auth_code, email, name):
  delete_params = {"authCode" : auth_code, "email" : email, "name" : name}
  delete_uri = URL + "/deleteCloudCredential"
  result = post(delete_uri, delete_params)
  return result


def perform_select(auth_code, email, filepattern):
  select_params = {"authCode" : auth_code, "email" : EMAIL, "filepattern" : filepattern}
  select_uri = URL + "/select"
  result = post(select_uri, select_params)
  return result


def perform_deselect(auth_code, email, filepattern):
  deselect_params = {"authCode" : auth_code, "email" : EMAIL, "filepattern" : filepattern}
  deselect_uri = URL + "/deselect"
  result = post(deselect_uri, deselect_params)
  return result


def perform_mapreduce(auth_code, email, mapcmd, groupby, reducecmd, filename):
  mapreduce_params = {"authCode" : auth_code, "email" : EMAIL, "map" : mapcmd, "groupby" : groupby, "reduce" : reducecmd, "targetfilename" : filename}
  mapreduce_uri = URL + "/mapreduce"

  result = post(mapreduce_uri, mapreduce_params)
  return result

def perform_terminate(auth_code, email, localname, typecode):
  term_params = {"authCode" : auth_code, "email" : email, "localname" : localname, "type" : typecode}
  terminate_uri = URL + "/terminate"
  result = post(terminate_uri, term_params)
  return result

def workflow1():
  auth_code = perform_authenticate()
  print perform_add_credential(auth_code, EMAIL, "aws", "aws1", S3KEY, S3PASS)
  print perform_load(auth_code, EMAIL, "s3://cloudinfra-west1/data/multi_part.txt", "amal", "aws1")
  print perform_store(auth_code, EMAIL, "s3://cloudinfra-west1/data/amal.txt", "amal", "aws1")
  print perform_delete_credential(auth_code, EMAIL, "aws1")

def workflow2():
  auth_code = perform_authenticate()
  print perform_add_credential(auth_code, EMAIL, "aws", "aws1", S3KEY, S3PASS)
  print perform_load(auth_code, EMAIL, "s3://cloudinfra-west1/data/multi_part.txt", "13k", "aws1")
  print perform_deselect(auth_code, EMAIL, ".*")
  print perform_select(auth_code, EMAIL, "13k")
  print perform_view(auth_code, EMAIL, "tail", "cat", 50000, 50000)
  print perform_delete_credential(auth_code, EMAIL, "aws1")

def workflow3():
  auth_code = perform_authenticate()
  print perform_add_credential(auth_code, EMAIL, "aws", "aws1", S3KEY, S3PASS)
  print perform_load(auth_code, EMAIL, "s3://cloudinfra-west1/data/btchunk3", "10m", "aws1")
  print perform_deselect(auth_code, EMAIL, ".*")
  print perform_select(auth_code, EMAIL, "10m")
  print perform_mapreduce(auth_code, EMAIL, 'cut -f5,6 -d"[" | grep -P "[A-Z][0-9][0-9][0-9]+" | grep cpums || true',
    'cut -f5,6 -d"[" |grep cpums | grep -o -P "[A-Z][0-9][0-9][0-9]+" || true',
    "grep -P -o \"cpums=[^ ]+\" | cut -f2 -d= | cut -f1 -d\"]\" | awk '{a+=$1;} END {print a;}'", "t9")

  print perform_deselect(auth_code, EMAIL, ".*")
  print perform_select(auth_code, EMAIL, "t9")
  print perform_view(auth_code, EMAIL, "tail", "sort", 5000000, 50000)
  print perform_delete_credential(auth_code, EMAIL, "aws1")

def lineanalysis():
  auth_code = perform_authenticate()
  print perform_add_credential(auth_code, EMAIL, "aws", "aws1", S3KEY, S3PASS)
  print perform_load(auth_code, EMAIL, "s3://cloudinfra-west2/shared/logs/apache_logs/logs/access_log", "apache", "aws1")
  print perform_select(auth_code, EMAIL, "apache")
  print perform_terminate(auth_code, EMAIL, "apache", 1)
  print perform_view(auth_code, EMAIL, r'''cut -f2 -d'['  | cut -f1 -d']' | cut -f1 -d: | python -c 'exec("from datetime import datetime; import sys;\nfor i in sys.stdin:\n  print datetime.strptime(i.strip(),\"%d/%b/%Y\").strftime(\"%a\")\n")' | sort | uniq -c''', r'''awk '{a[$2]+=$1} END{for(i in a) print a[i],i; }' | sort -nr -t: -k2''', 0, 5000)
  print perform_delete_credential(auth_code, EMAIL, "aws1")




if __name__ == '__main__':
  workflow1()
  workflow2()
  workflow3()

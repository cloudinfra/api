require 'rubygems'
require 'net/http'
require "addressable/uri"
require 'json'

API_KEY = "XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
API_SECRET = "YYYYYYYYYYYYYYYYYYYYYYYYYYYY"

EMAIL = "xyz@abc.com"
URL = "http://cloudinfra.in/api/v1"

def perform_authenticate
  auth_uri = URI(URL + "/authenticate")
  auth_params = {:apiKey => API_KEY, :apiSecret => API_SECRET, :email => EMAIL}
  res = Net::HTTP.post_form(auth_uri, auth_params)
  response_hash = JSON.parse(res.body)
  auth_code = response_hash["authCode"]
  auth_code
end

def perform_ls(auth_code, email, filepattern)
  ls_params = {:authCode => auth_code, :email => email, :filepattern => filepattern}
  ls_uri = URI(URL + "/ls")
  res = Net::HTTP.post_form(ls_uri, ls_params)
  response_hash = JSON.parse(res.body)
  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end
  response_hash["result"]
end

def ls
  auth_code = perform_authenticate
  puts perform_ls(auth_code, EMAIL, ".*")
end

def perform_store(auth_code, email, s3location, localfilename, credentialname)
  store_params = {:authCode => auth_code, :email => email, :s3location => s3location, :localfilename => localfilename, :credentialname => credentialname}
  store_uri = URI(URL + "/store")
  res = Net::HTTP.post_form(store_uri, store_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end

  response_hash["result"]
end

def store
  auth_code = perform_authenticate
  puts perform_store(auth_code, EMAIL, "s3://hello", "first_file", "first")
end

def perform_add_credential(auth_code, email, cloud_type, name, cloud_key, cloud_secret)
  add_params = {:authCode => auth_code, :email => EMAIL, :cloudType => cloud_type, :name => name, :cloudSecret => cloud_secret, :cloudKey => cloud_key}
  add_uri = URI(URL + "/addCloudCredential")
  res = Net::HTTP.post_form(add_uri, add_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end
  response_hash["result"]
end

def add_credential
  auth_code = perform_authenticate
  puts perform_add_credential(auth_code, EMAIL, "aws", "apiCloud", "cloudSecret", "cloudKey")
end

def perform_list_credentials(auth_code, email)
  list_params = {:authCode => auth_code, :email => email}
  list_uri = URI(URL + "/listCloudCredentials")
  res = Net::HTTP.post_form(list_uri, list_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end
  response_hash["result"]
end

def list_credentials
  auth_code = perform_authenticate
  puts perform_list_credentials(auth_code, EMAIL)
end

def perform_load(auth_code, email, s3location, localfilename, credentialname)
  load_params = {:authCode => auth_code, :email => email, :s3location => s3location, :localfilename => localfilename, :credentialname => credentialname}
  load_uri = URI(URL + "/load")
  res = Net::HTTP.post_form(load_uri, load_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end

  response_hash["result"]
end

def load
  auth_code = perform_authenticate
  puts perform_load(auth_code, EMAIL, "s3://location", "first", "first")
end

def perform_view(auth_code, email, distpipeline, centralpipeline, distsizelimit, centralsizelimit)
  view_params = {:authCode => auth_code, :email => email, :distpipeline => distpipeline, :centralpipeline => centralpipeline, :centralsizelimit => centralsizelimit, :distsizelimit => distsizelimit}
  view_uri = URI(URL + "/view")
  res = Net::HTTP.post_form(view_uri, view_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end

  response_hash["result"]
end

def view
  auth_code = perform_authenticate
  puts perform_view(auth_code, EMAIL, "cut", "head", 10, 10)
end

def perform_transform(auth_code, email, mappipeline, localfilename)
  transform_params = {:authCode => auth_code, :email => EMAIL, :mappipeline => mappipeline, :localfilename => localfilename}
  transform_uri = URI(URL + "/transform")
  res = Net::HTTP.post_form(transform_uri, transform_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end

  response_hash["result"]
end

def transform
  auth_code = perform_authenticate
  puts perform_transform(auth_code, EMAIL, "map", "local")
end

def perform_delete_credential(auth_code, email, name)
  delete_params = {:authCode => auth_code, :email => email, :name => name}
  delete_uri = URI(URL + "/deleteCloudCredential")
  res = Net::HTTP.post_form(delete_uri, delete_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end

  response_hash["result"]
end

def workflow1
  auth_code = perform_authenticate

  puts perform_add_credential(auth_code, EMAIL, "aws", "aws1", "AKIAIY3BMD4T6DI6MKAA", "7IS2ohmzidLmUBYDW/Vq6pmSCGasJRgDrEfXhCzg")

  puts perform_load(auth_code, EMAIL, "s3://cloudinfra-west1/data/multi_part.txt", "amal", "aws1")

  puts perform_store(auth_code, EMAIL, "s3://cloudinfra-west1/data/amal.txt", "amal", "aws1")

  puts perform_delete_credential(auth_code, EMAIL, "aws1")

end

def perform_select(auth_code, email, filepattern)
  select_params = {:authCode => auth_code, :email => EMAIL, :filepattern => filepattern}
  select_uri = URI(URL + "/select")
  res = Net::HTTP.post_form(select_uri, select_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end

  response_hash["result"]
end

def perform_deselect(auth_code, email, filepattern)
  deselect_params = {:authCode => auth_code, :email => EMAIL, :filepattern => filepattern}
  deselect_uri = URI(URL + "/deselect")
  res = Net::HTTP.post_form(deselect_uri, deselect_params)
  response_hash = JSON.parse(res.body)

  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end
  response_hash["result"]
end

def perform_mapreduce(auth_code, email, map, groupby, reduce, filename)
  mapreduce_params = {:authCode => auth_code, :email => EMAIL, :map => map, :groupby => groupby, :reduce => reduce, :targetfilename => filename}
  mapreduce_uri = URI(URL + "/mapreduce")
  
  res = Net::HTTP.post_form(mapreduce_uri, mapreduce_params)
  response_hash = JSON.parse(res.body)
  if response_hash["error"]
    puts "Error : " + response_hash["error"]
    return
  end

  response_hash["result"]
end

def workflow2
  auth_code = perform_authenticate
  puts perform_add_credential(auth_code, EMAIL, "aws", "aws1", "AKIAIY3BMD4T6DI6MKAA", "7IS2ohmzidLmUBYDW/Vq6pmSCGasJRgDrEfXhCzg")
  puts perform_load(auth_code, EMAIL, "s3://cloudinfra-west1/data/multi_part.txt", "13k", "aws1")
  puts perform_deselect(auth_code, EMAIL, ".*")
  puts perform_select(auth_code, EMAIL, "13k")
  puts perform_view(auth_code, EMAIL, "tail", "cat", 50000, 50000)
  puts perform_delete_credential(auth_code, EMAIL, "aws1")
end

def workflow3
  auth_code = perform_authenticate
  puts perform_add_credential(auth_code, EMAIL, "aws", "aws1", "AKIAIY3BMD4T6DI6MKAA", "7IS2ohmzidLmUBYDW/Vq6pmSCGasJRgDrEfXhCzg")
  puts perform_load(auth_code, EMAIL, "s3://cloudinfra-west1/data/btchunk3", "10m", "aws1")
  puts perform_deselect(auth_code, EMAIL, ".*")
  puts perform_select(auth_code, EMAIL, "10m")
  puts perform_mapreduce(auth_code, EMAIL, 'cut -f5,6 -d"[" | grep -P "[A-Z][0-9][0-9][0-9]+" | grep cpums || true',
    'cut -f5,6 -d"[" |grep cpums | grep -o -P "[A-Z][0-9][0-9][0-9]+" || true',
    "grep -P -o \"cpums=[^ ]+\" | cut -f2 -d= | cut -f1 -d\"]\" | awk '{a+=$1;} END {print a;}'", "t9")

  puts perform_deselect(auth_code, EMAIL, ".*")
  puts perform_select(auth_code, EMAIL, "t9")
  puts perform_view(auth_code, EMAIL, "tail", "sort", 5000000, 50000)
  puts perform_delete_credential(auth_code, EMAIL, "aws1")
end


def main
  #ls
  #list_credentials
  #add_credential
  #list_credentials
  #store
  #load
  #view
  #transform
  #workflow1
  #workflow2
  workflow3
end

main

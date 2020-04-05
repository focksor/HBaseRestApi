#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

from base64 import b64encode
from base64 import b64decode
import requests

'''
@File    :   hbase_rest_helper.py
@Time    :   2020/04/05 13:54:53
@Author  :   focksor
@Version :   1.0
@Contact :   focksor@outlook.com
@Desc    :   functions for hbase rest api.you should first run [hbase rest start] to enable hbase rest api.
'''


class HBaseRest:
    def __init__(self, host="localhost", port=8080):
        self.baseUrl = "http://" + host + ":" + str(port)

    def str2b64(self, source_str):
        return b64encode(bytes(source_str, encoding="utf8")).decode()

    def b642str(self, base64_bytes):
        return b64decode(base64_bytes).decode()

    def decode_data(self, src):
        row = src['Row']
        for r in row:
            r["key"] = self.b642str(r["key"])
            cell = r["Cell"]
            for c in cell:
                c["column"] = self.b642str(c["column"])
                c["$"] = self.b642str(c["$"])

    # get cluster version
    # return json like this: {'Version': '2.2.4'}
    def get_cluster_version(self):
        response = requests.get(self.baseUrl+'/version/cluster', headers={"Accept" : "application/json"})
        return response.json()

    # get cluster status    
    def get_cluster_status(self):
        response = requests.get(self.baseUrl+'/status/cluster', headers={"Accept" : "application/json"})
        return response.json()

    # get table list
    # return json like this: {'table': [{'name': 'test1'}, {'name': 'test2'}]}
    def get_table_list(self):
        response = requests.get(self.baseUrl+'/', headers={"Accept" : "application/json"})
        return response.json()

    # get table schema
    # return table schema json if successful
    # else, return status code
    def get_table_schema(self, table_name):
        response = requests.get(self.baseUrl+'/'+table_name+"/schema", headers={"Accept" : "application/json"})
        if response.status_code == 200:
            return response.json()
        return response.status_code

    # create a table if not exists, return status code 201 if successful.
    # add column_family to table if exists, return status code 200 if successful.
    # eg: create_table("test", "cf1", "cf2", "cf3")
    def create_table(self, table_name, *column_family_name):
        column_xml = ""
        for column in column_family_name:
            column_xml += '<ColumnSchema name="%s" />' % column
        table_xml = '<?xml version="1.0" encoding="UTF-8"?><TableSchema name="%s">%s</TableSchema>' % (table_name, column_xml)
        response = requests.post(self.baseUrl+'/%s/schema'%table_name, data=table_xml, headers = {'content-type': 'text/xml'})
        return response.status_code


    # drop a table named table_name
    # return status code 200 if successflu
    # return status code 404 if table does not exist.
    # eg: drop_table("test")
    def drop_table(self, table_name):
        response = requests.delete(self.baseUrl+'/%s/schema' % table_name)
        return response.status_code

    # insert data to table
    # create column if not exist
    # change data if exist
    # return status code 200 if successful
    # return status code 404 if table of column_family not found
    def put(self, table_name, column_family, column, row_key, data):
        column_b64 = self.str2b64(column_family+":"+column)
        row_key_b64 = self.str2b64(row_key)
        data_b64 = self.str2b64(data)
        data_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="%s"><Cell column="%s">%s</Cell></Row></CellSet>' % (row_key_b64, column_b64, data_b64)
        response = requests.put(self.baseUrl+'/%s/fakeRow' % table_name, data=data_xml, headers = {'content-type': 'text/xml'})

    # get data row from table
    # return status code 404 if not found
    # return json, values are encoded with base64 if base64decode is False.
    def get(self, table_name, row, column_family = None, column = None, base64decode=True):
        url = self.baseUrl+'/%s/%s' % (table_name, row)
        if column_family and column:
            url += "/%s:%s" % (column_family, column)

        response = requests.get(url, headers={"Accept" : "application/json"})
        if response.status_code == 200:
            res_json = response.json()
            if base64decode:
                self.decode_data(res_json)
            return res_json
        return response.status_code

    # scan data from table
    # return json if successful, values are encoded with base64 if base64decode is False.
    # else, return status code
    def scan(self, table_name, limit=100000, base64decode=True):
        scanner_xml = '<Scanner batch="%s"/>' % limit
        response = requests.put(self.baseUrl+'/%s/scanner' % table_name, data=scanner_xml, headers = {'content-type': 'text/xml'})
        if response.status_code == 201:
            location = response.headers['location']
            response = requests.get(location, headers={"Accept" : "application/json"})
            requests.delete(location, headers={"Accept" : "application/json"})
            res_json = response.json()
            if base64decode:
                self.decode_data(res_json)
            return res_json
        return response.status_code



if __name__ == "__main__":
    hbase = HBaseRest()
    print("cluster version:", hbase.get_cluster_version())
    print("cluster status:", hbase.get_cluster_status())
    print("get table list:", hbase.get_table_list())
    hbase.create_table("user", "userinfo")
    print("table list after adding:", hbase.get_table_list())
    print("table schema of user:", hbase.get_table_schema("user"))

    print("put data to user...")
    hbase.put("user", "userinfo", "phone", "0000001", "13800138000")
    hbase.put("user", "userinfo", "address", "0000001", "none")
    hbase.put("user", "userinfo", "phone", "0000002", "10086")

    print("get data from user:", hbase.get("user", "0000001"))
    print("get data from user:", hbase.get("user", "0000001", "userinfo", "phone"))
    print("get all data from user:", hbase.scan("user"))
    
    print("drop table user:", hbase.drop_table("user"))
    print("table list after droping:", hbase.get_table_list())


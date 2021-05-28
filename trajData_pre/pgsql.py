#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# encoding:utf=8

import psycopg2


def query(sqlstatement):
    result = {}
    conn = psycopg2.connect(database="AIS_db", user="postgres", password="postgres", host="127.0.0.1", port="5432")
    cur = conn.cursor()
    try:
        cur.execute(sqlstatement)
        rows = cur.fetchall()
        result['rows'] = rows
    except Exception as e:
        result['err'] = e
    finally:
        cur.close()
        return result

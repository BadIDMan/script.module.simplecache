#!/usr/bin/python
# -*- coding: utf-8 -*-

'''provides a simple stateless caching system for Kodi addons and plugins'''

import sys
import xbmcvfs
import xbmcgui
import xbmc
import xbmcaddon
import datetime
import time
import sqlite3
import json
from functools import reduce

ADDON_ID = "script.module.simplecache"

class SimpleCache(object):
    '''simple stateless caching system for Kodi'''
    enable_mem_cache = True
    data_is_json = False
    global_checksum = None
    _exit = False
    _auto_clean_interval = datetime.timedelta(hours=4)
    _win = None
    _busy_tasks = []
    _database = None

    def __init__(self):
        '''Initialize our caching class'''
        self._win = xbmcgui.Window(10000)
        self._monitor = xbmc.Monitor()
        self._database = None
        self.check_cleanup()
        self._log_msg("Initialized")

    def close(self):
        '''tell any tasks to stop immediately (as we can be called multithreaded) and cleanup objects'''
        self._exit = True
        while self._busy_tasks and not self._monitor.abortRequested():
            xbmc.sleep(25)

        if self._database:
            try:
                self._database.commit()
                self._database.close()
            except Exception:
                pass
            self._database = None

        del self._win
        del self._monitor
        self._log_msg("Closed")

    def __del__(self):
        if not self._exit:
            self.close()

    def get(self, endpoint, checksum="", json_data=False):
        checksum = self._get_checksum(checksum)
        cur_time = self._get_timestamp(datetime.datetime.now())
        result = None

        if self.enable_mem_cache:
            result = self._get_mem_cache(endpoint, checksum, cur_time, json_data)

        if result is None:
            result = self._get_db_cache(endpoint, checksum, cur_time, json_data)

        return result

    def set(self, endpoint, data, checksum="", expiration=datetime.timedelta(days=30), json_data=False):
        task_name = "set.%s" % endpoint
        self._busy_tasks.append(task_name)
        checksum = self._get_checksum(checksum)
        expires = self._get_timestamp(datetime.datetime.now() + expiration)

        if self.enable_mem_cache and not self._exit:
            self._set_mem_cache(endpoint, checksum, expires, data, json_data)

        if not self._exit:
            self._set_db_cache(endpoint, checksum, expires, data, json_data)

        self._busy_tasks.remove(task_name)

    def check_cleanup(self):
        cur_time = datetime.datetime.now()
        lastexecuted = self._win.getProperty("simplecache.clean.lastexecuted")
        if not lastexecuted:
            self._win.setProperty("simplecache.clean.lastexecuted", repr(cur_time))
        elif (eval(lastexecuted) + self._auto_clean_interval) < cur_time:
            self._do_cleanup()

    def _get_mem_cache(self, endpoint, checksum, cur_time, json_data):
        result = None
        cachedata = self._win.getProperty(endpoint)

        if cachedata:
            if json_data or self.data_is_json:
                cachedata = json.loads(cachedata)
            else:
                cachedata = eval(cachedata)
            if cachedata[0] > cur_time:
                if not checksum or checksum == cachedata[2]:
                    result = cachedata[1]
        return result

    def _set_mem_cache(self, endpoint, checksum, expires, data, json_data):
        cachedata = (expires, data, checksum)
        if json_data or self.data_is_json:
            cachedata_str = json.dumps(cachedata)
        else:
            cachedata_str = repr(cachedata)
        self._win.setProperty(endpoint, cachedata_str)

    def _get_db_cache(self, endpoint, checksum, cur_time, json_data):
        result = None
        query = "SELECT expires, data, checksum FROM simplecache WHERE id = ?"
        cache_data = self._execute_sql(query, (endpoint,))
        if cache_data:
            cache_data = cache_data.fetchone()
            if cache_data and cache_data[0] > cur_time:
                if not checksum or cache_data[2] == checksum:
                    if json_data or self.data_is_json:
                        result = json.loads(cache_data[1])
                    else:
                        result = eval(cache_data[1])
                    if self.enable_mem_cache:
                        self._set_mem_cache(endpoint, checksum, cache_data[0], result, json_data)
        return result

    def _set_db_cache(self, endpoint, checksum, expires, data, json_data):
        query = "INSERT OR REPLACE INTO simplecache( id, expires, data, checksum) VALUES (?, ?, ?, ?)"
        if json_data or self.data_is_json:
            data = json.dumps(data)
        else:
            data = repr(data)
        self._execute_sql(query, (endpoint, expires, data, checksum))

    def _do_cleanup(self):
        if self._exit or self._monitor.abortRequested():
            return
        self._busy_tasks.append(__name__)
        cur_time = datetime.datetime.now()
        cur_timestamp = self._get_timestamp(cur_time)
        self._log_msg("Running cleanup...")
        if self._win.getProperty("simplecachecleanbusy"):
            return
        self._win.setProperty("simplecachecleanbusy", "busy")

        query = "SELECT id, expires FROM simplecache"
        cursor = self._execute_sql(query)
        if cursor:
            for cache_data in cursor.fetchall():
                cache_id = cache_data[0]
                cache_expires = cache_data[1]

                if self._exit or self._monitor.abortRequested():
                    return

                self._win.clearProperty(cache_id)

                if cache_expires < cur_timestamp:
                    delete_query = 'DELETE FROM simplecache WHERE id = ?'
                    self._execute_sql(delete_query, (cache_id,))
                    self._log_msg("delete from db %s" % cache_id)

        self._execute_sql("VACUUM")

        self._busy_tasks.remove(__name__)
        self._win.setProperty("simplecache.clean.lastexecuted", repr(cur_time))
        self._win.clearProperty("simplecachecleanbusy")
        self._log_msg("Auto cleanup done")

    def _get_database(self):
        if self._database:
            return self._database

        addon = xbmcaddon.Addon(ADDON_ID)
        dbpath = addon.getAddonInfo('profile')
        dbfile = xbmcvfs.translatePath("%s/simplecache.db" % dbpath)

        if not xbmcvfs.exists(dbpath):
            xbmcvfs.mkdirs(dbpath)
        del addon

        try:
            connection = sqlite3.connect(
                dbfile,
                timeout=30,
                check_same_thread=False
            )
            connection.execute("PRAGMA journal_mode=WAL;")
            connection.execute("PRAGMA synchronous=NORMAL;")
            connection.execute("PRAGMA busy_timeout=30000;")
            connection.execute('SELECT * FROM simplecache LIMIT 1')
            self._database = connection
            return self._database
        except Exception:
            if xbmcvfs.exists(dbfile):
                xbmcvfs.delete(dbfile)
            connection = sqlite3.connect(
                dbfile,
                timeout=30,
                check_same_thread=False
            )
            connection.execute("""CREATE TABLE IF NOT EXISTS simplecache(
                    id TEXT UNIQUE, expires INTEGER, data TEXT, checksum INTEGER)""")
            connection.execute("PRAGMA journal_mode=WAL;")
            connection.execute("PRAGMA synchronous=NORMAL;")
            connection.execute("PRAGMA busy_timeout=30000;")
            self._database = connection
            return self._database

    def _execute_sql(self, query, data=None):
        retries = 0
        last_error = None

        _database = self._get_database()

        while retries < 10 and not self._monitor.abortRequested():
            if self._exit:
                return None
            try:
                if isinstance(data, list):
                    result = _database.executemany(query, data)
                elif data:
                    result = _database.execute(query, data)
                else:
                    result = _database.execute(query)

                _database.commit()
                return result

            except sqlite3.OperationalError as exc:
                last_error = exc
                if "locked" in str(exc).lower():
                    self._log_msg("retrying DB commit...")
                    retries += 1
                    self._monitor.waitForAbort(0.5)
                else:
                    break

            except Exception as exc:
                last_error = exc
                break

        if last_error:
            self._log_msg("_database ERROR ! -- %s" % str(last_error), xbmc.LOGWARNING)

        return None

    @staticmethod
    def _log_msg(msg, loglevel=xbmc.LOGDEBUG):
        xbmc.log("Skin Helper Simplecache --> %s" % msg, level=loglevel)

    @staticmethod
    def _get_timestamp(date_time):
        return int(time.mktime(date_time.timetuple()))

    def _get_checksum(self, stringinput):
        if not stringinput and not self.global_checksum:
            return 0
        if self.global_checksum:
            stringinput = "%s-%s" %(self.global_checksum, stringinput)
        else:
            stringinput = str(stringinput)
        return reduce(lambda x, y: x + y, map(ord, stringinput))


def use_cache(cache_days=14):
    def decorator(func):
        def decorated(*args, **kwargs):
            method_class = args[0]
            method_class_name = method_class.__class__.__name__
            cache_str = "%s.%s" % (method_class_name, func.__name__)
            for item in args[1:]:
                cache_str += u".%s" % item
            cache_str = cache_str.lower()
            cachedata = method_class.cache.get(cache_str)
            global_cache_ignore = False
            try:
                global_cache_ignore = method_class.ignore_cache
            except Exception:
                pass
            if cachedata is not None and not kwargs.get("ignore_cache", False) and not global_cache_ignore:
                return cachedata
            else:
                result = func(*args, **kwargs)
                method_class.cache.set(cache_str, result, expiration=datetime.timedelta(days=cache_days))
                return result
        return decorated
    return decorator

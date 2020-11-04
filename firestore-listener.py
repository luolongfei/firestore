#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author mybsdc <mybsdc@gmail.com>
@date 2020/10/12
@time 15:05

@issues
    https://github.com/googleapis/python-firestore/issues/18
    https://github.com/firebase/firebase-admin-python/issues/294
    https://github.com/firebase/firebase-admin-python/issues/282
    https://stackoverflow.com/questions/55876107/how-to-detect-realtime-listener-errors-in-firebase-firestore-database
    https://uyamazak.hatenablog.com/entry/2019/07/09/221041
"""

import os
import argparse
import sys
import json
import base64
import time
import datetime
import traceback
from concurrent.futures import ThreadPoolExecutor
from google.cloud import firestore
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from google.api_core.exceptions import GoogleAPIError
from loguru import logger
import logging


def catch_exception(origin_func):
    def wrapper(self, *args, **kwargs):
        """
        用于异常捕获的装饰器
        :param origin_func:
        :return:
        """
        try:
            return origin_func(self, *args, **kwargs)
        except AssertionError as e:
            logger.error('参数错误：{}', str(e))
        except GoogleAPIError as e:
            logger.error('GoogleAPIError: ', str(e))
        except Exception as e:
            logger.error('出错：{} 位置：{}', str(e), traceback.format_exc())
        finally:
            pass

    return wrapper


class FirestoreListener(object):
    @logger.catch
    def __init__(self):
        FirestoreListener.check_py_version()

        # 命令行参数
        self.args = self.get_all_args()

        # 日志
        self.__logger_setting()

        # Firestore 日志：由于 firestore 的异常和日志是在它自己的子进程中处理的，外层无法捕获错误信息，但是 firestore 使用了 logging 模块写日志，故可将日志记录在文件中
        logging.basicConfig(filename='logs/firestore.log', level=logging.DEBUG if self.args.debug else logging.INFO,
                            format='[%(asctime)s] %(levelname)s | %(process)d:%(filename)s:%(name)s:%(lineno)d:%(module)s - %(message)s')

        # firestore 数据库配置
        self.collection_id = self.args.collection_id
        self.col_watch = None

        self.is_first_time = True
        self.today = FirestoreListener.today()

        # 线程池
        self.max_workers = self.args.max_workers
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=self.max_workers)

    @staticmethod
    def today():
        return str(datetime.date.today())

    def __logger_setting(self) -> None:
        logger.remove()

        level = 'DEBUG' if self.args.debug else 'INFO'
        format = '<green>[{time:YYYY-MM-DD HH:mm:ss.SSS}]</green> <b><level>{level: <8}</level></b> | <cyan>{process.id}</cyan>:<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>'

        logger.add('logs/{time:YYYY-MM-DD}.log', level=level, format=format, encoding='utf-8')
        logger.add(sys.stderr, colorize=True, level=level, format=format)

    def __get_db(self):
        return firestore.Client.from_service_account_json(self.args.key_path)

    @staticmethod
    def check_py_version(major=3, minor=6):
        if sys.version_info < (major, minor):
            raise UserWarning(f'请使用 python {major}.{minor} 及以上版本，推荐使用 python 3.8')

    @staticmethod
    def get_all_args():
        """
        获取所有命令行参数
        :return:
        """
        parser = argparse.ArgumentParser(description='需要传给 FirestoreListener 的各种参数及其含义',
                                         epilog='e.g. python3.8 -i firestore-listener.py -k=luolongfei-1ad2ca735e37.json')
        parser.add_argument('-c', '--collection_id', help='collection 名称，或者叫 collection id',
                            default='Message', type=str)
        parser.add_argument('-k', '--key_path',
                            help='由谷歌提供的 json 格式的密钥文件的路径，更多信息参考：https://googleapis.dev/python/google-api-core/latest/auth.html',
                            required=True, type=str)
        parser.add_argument('-mw', '--max_workers', help='最大线程数（在执行外部 php 命令时）', default=1, type=int)
        parser.add_argument('-d', '--debug', help='是否开启 Debug 模式', action='store_true')

        return parser.parse_args()

    @staticmethod
    def __json_helper(obj):
        if isinstance(obj, DatetimeWithNanoseconds):
            return obj.timestamp()

        raise TypeError(f'{type(obj)} 类型不可序列化为 json')

    @staticmethod
    def __php_run(document: dict) -> None:
        """
        执行外部 php 命令
        :param document:
        :return:
        """
        try:
            doc_json = json.dumps(document, default=FirestoreListener.__json_helper, ensure_ascii=False).encode('utf-8')
            doc_b64 = base64.b64encode(doc_json).decode('utf-8')
            cmd = "php artisan command:fcmpushformessage '{}'".format(doc_b64)

            status_code = os.system(cmd)
            if status_code != 0:
                logger.error('执行外部命令出错：{}', cmd)
        except Exception as e:
            logger.error('构造外部命令出错：{}', str(e))

    @logger.catch
    def __on_snapshot(self, col_snapshot, changes, read_time) -> None:
        """
        Firestore 回调
        新增文档时触发执行外部命令
        :param col_snapshot:
        :param changes:
        :param read_time:
        :return:
        """
        # 常驻执行，更新日志目录
        real_today = FirestoreListener.today()
        if self.today != real_today:
            self.today = real_today
            self.__logger_setting()

        for change in changes:
            if change.type.name == 'ADDED':
                if self.is_first_time:
                    self.is_first_time = False

                    return

                # 将任务添加到线程池
                self.thread_pool_executor.submit(FirestoreListener.__php_run, change.document.to_dict())

                logger.debug('新增文档 ID: {} 内容: {}', change.document.id, change.document.to_dict())
            elif change.type.name == 'MODIFIED':
                logger.debug('修改文档 ID: {} 内容: {}', change.document.id, change.document.to_dict())
            elif change.type.name == 'REMOVED':
                logger.debug('移除快照或文档 ID: {} 内容: {}', change.document.id, change.document.to_dict())

    @logger.catch
    def __start_snapshot(self):
        self.col_watch = self.__get_db().collection(self.collection_id).order_by('updatedAt',
                                                                                 direction=firestore.Query.DESCENDING).limit(
            1).on_snapshot(self.__on_snapshot)

    @logger.catch
    def __listen_for_changes(self) -> None:
        """
        监听文档变化
        on_snapshot 方法在每次新增文档时候，会移除旧的快照，创建新的快照
        :return:
        """
        self.__start_snapshot()

        while True:
            if self.col_watch._closed:
                logger.error('检测到 firestore 很不仗义的罢工了，将尝试重启')

                try:
                    self.__start_snapshot()

                    # 防止异常导致宕机
                    time.sleep(1)
                except Exception as e:
                    logger.error('重启失败：{}', str(e))
                    break

            time.sleep(0.001)

    @logger.catch
    @catch_exception
    def run(self):
        self.__listen_for_changes()


if __name__ == '__main__':
    firestore_listener = FirestoreListener()
    firestore_listener.run()

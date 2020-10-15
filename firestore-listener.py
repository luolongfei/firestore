#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author mybsdc <mybsdc@gmail.com>
@date 2020/10/12
@time 15:05
"""

import os
import argparse
import sys
import json
import base64
import traceback
import subprocess
from concurrent.futures import ThreadPoolExecutor
from google.cloud import firestore
from google.api_core.datetime_helpers import DatetimeWithNanoseconds
from loguru import logger


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
        except Exception as e:
            logger.error('出错：{} 位置：{}', str(e), traceback.format_exc())
        finally:
            pass

    return wrapper


class FirestoreListener(object):
    def __init__(self):
        FirestoreListener.check_py_version()

        # 命令行参数
        self.args = self.get_all_args()

        # 日志
        logger.remove()
        logger.add('logs/{time:YYYY-MM-DD}.log', filter=FirestoreListener.no_debug_log, encoding='utf-8')
        logger.add(sys.stderr, colorize=True, level='DEBUG' if self.args.debug else 'INFO',
                   format='<green>[{time:YYYY-MM-DD HH:mm:ss.SSS}]</green> <b><level>{level: <8}</level></b> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>')

        # 初始化 firestore 数据库
        self.db = firestore.Client.from_service_account_json(self.args.key_path)
        self.collection_id = self.args.collection_id

        self.is_first_time = True

        # 线程池
        self.max_workers = self.args.max_workers
        self.thread_pool_executor = ThreadPoolExecutor(max_workers=self.max_workers)

    @staticmethod
    def no_debug_log(record: dict) -> bool:
        return record['level'].name != 'DEBUG'

    @staticmethod
    def check_py_version(major=3, minor=6):
        if sys.version_info.major != major or sys.version_info.minor < minor:
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
        parser.add_argument('-mw', '--max_workers', help='最大线程数（在执行外部 php 命令时）', default=20, type=int)
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

    def __on_snapshot(self, col_snapshot, changes, read_time) -> None:
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

    def __listen_for_changes(self) -> None:
        """
        监听文档变化
        on_snapshot 方法在每次新增文档时候，会移除旧的快照，创建新的快照
        :return:
        """
        col_ref = self.db.collection(self.collection_id).order_by('updatedAt',
                                                                  direction=firestore.Query.DESCENDING).limit(1)
        col_watch = col_ref.on_snapshot(self.__on_snapshot)

    @staticmethod
    def time_diff(start_time, end_time):
        """
        计算时间间隔
        :param start_time: 开始时间戳
        :param end_time: 结束时间戳
        :return:
        """
        diff_time = end_time - start_time

        if diff_time < 0:
            raise ValueError('结束时间必须大于等于开始时间')

        if diff_time < 60:
            return '{:.2f}秒'.format(diff_time)
        else:
            diff_time = int(diff_time)

        if 60 <= diff_time < 3600:
            m, s = divmod(diff_time, 60)

            return '{:02d}分钟{:02d}秒'.format(m, s)
        elif 3600 <= diff_time < 24 * 3600:
            m, s = divmod(diff_time, 60)
            h, m = divmod(m, 60)

            return '{:02d}小时{:02d}分钟{:02d}秒'.format(h, m, s)
        elif 24 * 3600 <= diff_time:
            m, s = divmod(diff_time, 60)
            h, m = divmod(m, 60)
            d, h = divmod(h, 24)

            return '{:02d}天{:02d}小时{:02d}分钟{:02d}秒'.format(d, h, m, s)

    @catch_exception
    def run(self):
        self.__listen_for_changes()


if __name__ == '__main__':
    firestore_listener = FirestoreListener()
    firestore_listener.run()

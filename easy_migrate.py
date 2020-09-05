
import pymysql,json
import numpy as np
import pandas as pd
from sshtunnel import SSHTunnelForwarder

class EasyMigrate():
    '''小型数据库迁移工具'''
    def __init__(self, options:dict):
        '''初始化配置参数'''
        print('init')
        self.conf_source = options['db']['source']
        self.conf_target = options['db']['target']
        self.server = {}
        self.connect = {}
        self._set_connection_('source', self.conf_source)
        self._set_connection_('target', self.conf_target)
        self.tasks = []
        self.errdict = {}
        self.mapping = None

        if options.get('mode'):
            self.mode = options.get('mode')
        else:
            self.mode = 'insert'

    def _set_connection_(self, type:str, conn_config:dict):
        '''设置链接'''
        if conn_config.get('ssh'):
            print(f'start ssh server connect to {type}')
            self.server[type] = SSHTunnelForwarder(**conn_config.get('ssh'))
            self.server[type].start()
        print(f'connecting to {type}')
        self.connect[type] = pymysql.connect(**conn_config['connect'])
    def append_task(self,sql:str, target_table_name:str,task_name=None):
        '''添加一个任务，sql查询语句，目标表名字'''
        if not task_name:
            i = len(self.tasks) + 1
            task_name = f'task_{i}'
        self.tasks.append({
            'status':0,
            'task_name':task_name,
            'sql':sql,
            'target_table_name':target_table_name
        })
    def before_reading(self,sql,task_name):
        '''读取源数据前修改sql语句，比如加入通用limit'''
        return sql
    def after_reading(self,df,task_name):
        '''供用户改写读取数据后的清洗工作'''
        return df
    def run(self):
        '''开始任务'''
        length = len(self.tasks)
        i = 1
        for task in self.tasks:
            task_name = task['task_name']
            sql = self.before_reading(task['sql'],task_name)
            df = self.after_reading(
                pd.read_sql_query(sql, self.connect['source']),
                task_name
                )
            target_table_name = task['target_table_name']
            print(f'start task {i}:\ndata count:\n{len(df)}\nto {target_table_name}')
            self.process_update(
                df, 
                target_table_name,
                task_name)
        
        self.server['source'].stop()
        self.connect['source'].close()
        print('all success !')

    def get_target_table_type(self,target_table_name):
        '''获取目标表结构'''
        sql = f'''select COLUMN_NAME,DATA_TYPE from information_schema.columns
                where table_name = '{target_table_name}';'''
        df = pd.read_sql_query(sql, self.connect['target'])
        return df.set_index('COLUMN_NAME')

    def process_update(self,df,target_table_name,task_name):
        '''处理数据更新'''
        length = len(df)
        if length == 0:
            print('没数据')
            return
        keys = df.iloc[0].keys()
        keys_str = ','.join(keys)
        # 获取表结构，用来识别字段是否要添加引号
        columns = self.get_target_table_type(target_table_name)
        cursor = self.connect['target'].cursor()
        errinfo = []
        if self.mode == 'clear_insert':
            print(f'clear table {target_table_name}')
            sql = f'delete from {target_table_name}'
            cursor.execute(sql)
            self.connect['target'].commit()
        print('start insert data ...')
        length = len(df)
        step = int(length/5)
        for i in range(length):
            if i%step==0:
                print(f'{i}/{length}')
            line = df.iloc[i]
            values = []
            for key in keys:
                val = line[key]
                data_type = columns.loc[key,'DATA_TYPE']
                if 'series' in str(type(data_type)):
                    data_type = data_type.iloc[0]                

                if pd.isna(val):
                    val = 'null'
                elif val is None:
                    val = 'null'
                    # elif type(val) == 
                elif data_type in ['varchar','blob','char','date','datetime','longblob','linestring']:
                    # 需要添加引号的字段格式
                    val = f'"{val}"'
                elif 'text' in data_type:
                    val = f'"{val}"'
                else:
                    val = f'{val}'
                values.append(val)
            values_str = ','.join(values)
            sql = f'insert into {target_table_name} ({keys_str}) values ({values_str})'
            try:
                cursor.execute(sql)
            except Exception as e:
                print(e)
                # print(sql)
                errinfo.append({
                    'error':e.args[0],
                    'error_info':e.args[1],
                    'sql':sql,
                    'line':line
                })
        
            # print(sql)
        print(f'commit table:{target_table_name} / task:{task_name}')
        self.connect['target'].commit()
        if errinfo:
            self.errdict.update({
                task_name:errinfo
            })
        return df





## Easy Migrate
> 一个轻量级数据库迁移工具

### 核心思想
> 用一条sql语句导出一个表，然后导入到目标库的表中。

### 使用方法
0. 安装依赖包
1. 引入类
2. 配置连接参数
3. 添加任务
4. 运行
   
```python
# 1. 引入类
from easy_migrate import EasyMigrate

# 2. 配置数据库连接
config = {
    'db':{
        'source':{
            # 数据源，如果需要ssh连接才配置ssh参数，否则直接配置connect即可
            'ssh':{
                'ssh_address_or_host':('ssh服务器ip地址',22),
                'ssh_username':'root',
                'ssh_pkey':'D:/id_rsa',
                'remote_bind_address':('127.0.0.1',3306), # 远程数据库地址
                'local_bind_address':('127.0.0.1',1234),  # 本地监听地址
            },
            'connect': {
                'host':'127.0.0.1',
                'port':1234,
                'user':'root',
                'passwd':'XXX8888你的密码',
                'db':'原始数据库'
            }
        },
        'target':{
            # 目标
            'connect': {
                'host':'目标库地址',
                'port':3306,
                'user':'root',
                'passwd':'目标库密码',
                'db':'目标数据库'
            }
        }
    },
    'mode':'clear_insert', # 清空后插入,默认是直接插入
}

test = EasyMigrate(config)

# 用户表
sql = '''
    SELECT 
        id,
        '' as 'password',
        null as 'last_login',
        0 as 'is_superuser',
        id as 'username',
        '' as 'first_name',
        '' as 'last_name',
        '' as 'email',
        0 as 'is_staff',
        1 as 'is_active',
        from_unixtime(time) as 'date_joined',
        2 as 'role_group',
        '' as 'name',
        '' as 'name_pinyin',
        (case WHEN `sex` = '1' THEN 1 WHEN `sex` = '2' THEN 0 ELSE null	END) as 'sex',
        nickname,
        '' as 'nickname_pinyin'
        ,(case WHEN `user` REGEXP '^[0-9]{11}$' THEN `user` ELSE null END) as 'cell_phone',
        province,
        city,
        '' as 'district',
        '' as 'address',
        null as 'unionid',
        (case WHEN `type`='2' THEN openid END) as 'qq_openid',
		(case WHEN `type`='3' THEN openid END) as 'wxweb_openid',
        null as 'wxapp_openid',
        img as 'avatar_url',
        null as 'invite_id',
        type as 'reg_origin',
        0 as 'number_of_visits',
        0 as 'last_device_id',
        null as 'pro_store_id',
        0 as 'wx_receiver',
        0 as 'subscribe',
        null as 'subscribe_time'
    from old_users
    ORDER BY id
    -- limit 240
'''

# 添加任务
test.append_task(sql,'account_useraccount','用户表')

# 执行任务
test.run()
```

### 生命周期

1. before_reading 读取数据源前处理sql语句，比如插入通用limit或者一些合法性检查。
2. after_reading 读取数据后的清洗工作，合并数据等操作

> 如果需要控制生命周期的两个处理阶段，需要自己写一个类，继承EasyMigrate类
> 然后改写上述两个方法

### 例子
```python
from easy_migrate import EasyMigrate
class MyProject(EasyMigrate):
    def after_reading(self,df,task_name):
        '''供用户改写读取数据后的清洗工作'''
        # 第一个表处理用户数据，发现有不少用户的手机号是重复的，造成后面几个业务表的数据要做合并
        if self.errdict and self.errdict.get('用户表'):
            # 有错误信息
            if task_name in ['会员记录','钱包余额','订单','订阅']:
                # 合并user_id
                if not self.mapping:
                    allerrlist = self.errdict.get('用户表')
                    self.mapping = self.mapping_user(allerrlist) # 这里是做了一个根据无法导入的用户数据（重复手机号等），进行业务数据的user_id合并函数，你自己根据业务来写吧。
                mapping = self.mapping
                print(f'处理可能存在的重复用户订单信息：{task_name}')
                for row in df.index:
                    for key,value in mapping.items():
                        user_id = int(df.loc[row,'user_id'])
                        if user_id in value:
                            # 把user_id改为已经存在的user_id
                            print(f'发现userid={user_id},改为{int(key)}')
                            df.loc[row,'user_id'] = int(key)
        return df
```
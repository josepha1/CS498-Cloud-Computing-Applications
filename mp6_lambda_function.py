import os
import json

import redis
import pymysql
import config
import sys
import json

rds_host = "mp6-database1.cluster-ro-ccdifadvhwex.us-east-1.rds.amazonaws.com"
rds_host_writer = "mp6-database1-instance-1.ccdifadvhwex.us-east-1.rds.amazonaws.com"
db_name = "mp6_db1"
                           
class DB:
    def __init__(self, **params):
        params.setdefault("charset", "utf8mb4")
        params.setdefault("cursorclass", pymysql.cursors.DictCursor)

        self.mysql = pymysql.connect(**params)

    def query(self, sql):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    def record(self, sql, values):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql, values)
            return cursor.fetchone()

    def insert(self, sql, values):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql, values)
            self.mysql.commit()
            return

    def delete(self, sql):
        with self.mysql.cursor() as cursor:
            cursor.execute(sql)
            self.mysql.commit()
            return

# Time to live for cached data
TTL = 10

# Read the Redis credentials from the REDIS_URL environment variable.
REDIS_URL = 'redis://mp6-redis-cluster.yvcyzt.ng.0001.use1.cache.amazonaws.com:6379'

# Initialize the database
Database = DB(host=rds_host,user=config.name,
                          passwd=config.password,db=db_name,
                          connect_timeout=5,
                          cursorclass=pymysql.cursors.DictCursor)

Database_writer = DB(host=rds_host_writer,user=config.name,
                          passwd=config.password,db=db_name,
                          connect_timeout=5,
                          cursorclass=pymysql.cursors.DictCursor)

# Initialize the cache
Cache = redis.Redis.from_url(REDIS_URL)

def fetch(sql):
    """Retrieve records from the cache, or else from the database."""
    res = Cache.get(sql)

    if res:
        return json.loads(res)

    res = Database.query(sql)
    Cache.setex(sql, TTL, json.dumps(res))
    return res


def hero(id, use_cache):
    """Retrieve a record from the cache, or else from the database."""
    key = f"hero:{id}"
    # key = f"hero:{new_hero['name']}"
    res = Cache.hgetall(key)

    if res and use_cache:
        print('from cache: ' + str(res))
        return res

    # sql = "SELECT `id`, `name` FROM `planet` WHERE `id`=%s"
    sql = "SELECT `id`, `hero`, `power`, `name`, `xp`, `color` FROM `heroes` WHERE `id`=%s"
    res = Database.record(sql, (id,))
    print('from db:' + str(res))
    if not res:
        res = Database_writer.record(sql, (id,))
        print('from writer db:' + str(res))
        print('query: ' + sql + ', id=' + str(id))

    if res:
        Cache.hmset(key, res)
        Cache.expire(key, TTL)

    return res

def add_hero(id, new_hero, use_cache):
    print('add new hero:' + str(id) + ' ' + str(new_hero))
    sql = """insert into heroes (id, hero, power, name, xp, color) values (""" + str(id) + """, %s, %s, %s, %s, %s)"""
    # print(sql)
    values = (new_hero['hero'], new_hero['power'], new_hero['name'], new_hero['xp'], new_hero['color'])
    Database_writer.insert(sql, values)
    # res = hero(id, use_cache)
    if use_cache:
        final_hero = dict()
        final_hero['id'] = id
        final_hero.update(new_hero)
        key = f"hero:{id}"
        Cache.hmset(key, final_hero)
        Cache.expire(key, TTL)

    print('hero added successfully:' + str(hero(id, True)))

def lambda_handler(event=None, context=None):
    print("event: " + str(event))
    # print('mp6 test print')
    if "DELETE" in event:
        Database_writer.delete('delete from heroes where id > 25')
        return_val = {
            "statusCode": 200,
            "body": 'delete success'
        }
        return return_val
    
    # print(body)
    use_cache = False
    if event['USE_CACHE'] == 'True':
        print('use cache = true')
        use_cache = True
    
    read_request = False
    if event['REQUEST'] == 'read':
        print('read req')
        read_request = True
    
    sqls = event['SQLS']
    heroes = dict()
    # body = '['
    body = list()
    if read_request:
        for id in sqls:
            body.append(hero(id, use_cache))
            # body = body + json.dumps(hero(id, use_cache)) + ','
        # body = body + ']'
    else:
        # Database_writer.delete('delete from heroes where id > 25')
        last_id = Database_writer.query("select max(id) as last_id from heroes")[0]["last_id"]
        print('last id = ' + str(last_id))
        new_id = last_id + 1
        for new_hero in sqls:
            add_hero(new_id, new_hero, use_cache)
            new_id = new_id + 1
        body = 'write success'
            

    return_val = {
        "statusCode": 200,
        "body": body #json.dumps(heroes.values())
    }
    print('return: ' + str(return_val))
    # return "lambda_handler called successfully, sqls:" + str(heroes)
    return return_val
    # return "lambda_handler called successfully, sqls:" + str(heroes[sqls[1]])

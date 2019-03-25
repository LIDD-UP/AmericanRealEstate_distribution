# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: redis_insert_test.py
@time: 2019/2/27
"""

def get_detail_url():
    from crawl_tools.get_sql_con import get_sql_con
    import redis
    pool = redis.ConnectionPool(host='127.0.0.1',
                                # password='123456'
                                )
    redis_pool = redis.Redis(connection_pool=pool)
    conn = get_sql_con()
    cursor = conn.cursor()
    sql_string = '''
        SELECT
    	property_id
    FROM
    	tb_realtor_detail_json 
    	limit 10,30
    '''
    cursor.execute(sql_string)
    for result in cursor.fetchall():
        redis_pool.lpush('realtor:property_id','http://{}'.format(result[0]))
        # redis_pool.lpush('realtor:property_id', result[0])
    cursor.close()
    conn.commit()
    conn.close()


get_detail_url()

# from crawl_tools.get_psql_con import get_psql_con
# import redis
# pool = redis.ConnectionPool(host='127.0.0.1',
#                             # password='123456'
#                             )
# redis_pool = redis.Redis(connection_pool=pool)

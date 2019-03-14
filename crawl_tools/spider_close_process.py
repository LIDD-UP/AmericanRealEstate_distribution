# -*- coding:utf-8 _*-  
""" 
@author:Administrator
@file: spider_close_process.py
@time: 2019/3/14
"""
from crawl_tools.get_sql_con import get_sql_con


class SpiderCloseProcess(object):

    def update_detail_data(self, conn, batch_size):
        print("更新detail数据")
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()
        realtor_update_property_id_sql_str = '''
            SELECT
                rl.property_id,
                rl.address,
                rl.lat,
                rl.lon,
                rl.beds,
                rl.sqft,
                rl.baths,
                rl.price,
                rl.lot_size 
            FROM
                tb_realtor_list_page_json_splite rl
                INNER JOIN tb_realtor_detail_json rd ON rl.property_id = rd.property_id 
            WHERE
            rl.address != rd.address 
            OR rl.address != rd.address
            OR rl.lat != rd.lat
            OR rl.lon != rd.lon
            OR rl.beds != rd.beds
            OR rl.sqft != rd.sqft
            OR rl.baths != rd.baths
            OR rl.price != rd.price
            OR rl.lot_size != rd.lot_size
        '''

        # 获取需要更新的数据
        results1 = cursor1.execute(realtor_update_property_id_sql_str)

        # 批量更新数据
        sql_string1 = '''
            UPDATE tb_realtor_detail_json rj 
            SET is_dirty = '1',
            rj.address = %s,
            last_operation_date = now(),
            rj.lat = %s,
            rj.lon = %s,
            rj.beds = %s,
            rj.sqft = %s,
            rj.baths = %s,
            rj.price = %s,
            rj.lot_size = %s
            WHERE
                rj.property_id =%s
        '''


        print('更新跟新了{}'.format(cursor1.rowcount))

        sql_string_list = []
        update_data_number = cursor1.rowcount
        update_count_number = 0
        update_count = int(update_data_number/batch_size)
        remainder_update_rows = update_data_number % batch_size

        for i in cursor1.fetchall():
            if update_count_number == update_count and remainder_update_rows !=0:
                # print(i)
                print([i[1], i[2], [3], [4], [5], [6], [7], [8], i[0]])
                sql_string_list.append([i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[0]])
                if len(sql_string_list) == remainder_update_rows:
                    cursor2.executemany(sql_string1, sql_string_list)
                    conn.commit()
                    sql_string_list = []

            if update_count_number < update_count:
                # print(i)
                print(i)
                print([i[1], i[2], [3], [4], [5], [6], [7], [8], i[0]])
                sql_string_list.append([i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[0]])
                if len(sql_string_list) == batch_size:
                    cursor2.executemany(sql_string1, sql_string_list)
                    conn.commit()
                    update_count_number += 1
                    sql_string_list = []

    def splite_list_data(self, conn):
        print("拆分数据")
        cursor = conn.cursor()
        sql_string_splite = '''
                INSERT INTO tb_realtor_list_page_json_splite ( property_id, address, last_operation_date,lat, lon,beds,sqft,baths,price,lot_size) 
            (
            SELECT
                n_table.property_id,
                n_table.address,
                n_table.last_operation_date,
                n_table.lat,
                n_table.lon,
                n_table.beds,
                n_table.sqft,
                n_table.baths,
                n_table.price,
                n_table.lot_size
            FROM
                (
                SELECT
                    cast( JSON_EXTRACT( rj.json_data, '$.property_id' ) AS SIGNED ) AS property_id,
                    JSON_EXTRACT( rj.json_data, '$.address' ) AS address,
                    now( ) AS last_operation_date,
                    JSON_EXTRACT( rj.json_data, '$.lat' ) AS lat,
                    JSON_EXTRACT( rj.json_data, '$.lon' ) AS lon,
                    JSON_EXTRACT( rj.json_data, '$.beds' ) AS beds,
                    JSON_EXTRACT( rj.json_data, '$.sqft' ) AS sqft,
                    JSON_EXTRACT( rj.json_data, '$.baths' ) AS baths,
                    JSON_EXTRACT( rj.json_data, '$.price' ) AS price,
                    JSON_EXTRACT( rj.json_data, '$.lot_size' ) AS lot_size
        
                    
                FROM
                    tb_realtor_list_page_json rj 
                ) n_table 
            WHERE
                n_table.property_id IS NOT NULL 

        
            GROUP BY n_table.property_id 
            )
        '''
        cursor.execute(sql_string_splite)
        print("拆分了{}条数据".format(cursor.rowcount))
        conn.commit()

    def insert_detail_data(self, conn):
        print('插入detail没有的property_id')
        cursor = conn.cursor()
        sql_string_insert = '''
            INSERT INTO tb_realtor_detail_json ( property_id,address,is_dirty, last_operation_date,lat, lon,beds,sqft,baths,price,lot_size) (
                SELECT
                    rl.property_id ,
                    rl.address ,
                    0,
                    now(),
                    rl.lat,
                    rl.lon,
                    rl.beds,
                    rl.sqft,
                    rl.baths,
                    rl.price,
                    rl.lot_size
                FROM
                    tb_realtor_list_page_json_splite rl
                    LEFT JOIN tb_realtor_detail_json rd ON rl.property_id = rd.property_id 
                WHERE
                    rd.property_id IS NULL
                )
        '''
        cursor.execute(sql_string_insert)
        print("插入detail 表没有的数据：{}条".format(cursor.rowcount))
        conn.commit()

    def delete_not_exit(self, conn, batch_size):
        print("更新detail数据")
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()
        realtor_update_property_id_sql_str = '''
            SELECT
                rl.property_id
            FROM
                tb_realtor_detail_json rl
                LEFT JOIN tb_realtor_list_page_json_splite rd ON rl.property_id = rd.property_id 
            WHERE
                rd.property_id IS NULL
        '''

        # 获取需要更新的数据
        results1 = cursor1.execute(realtor_update_property_id_sql_str)

        # 批量更新数据
        sql_string1 = '''
            DELETE FROM tb_realtor_detail_json 
            WHERE property_id =%s
        '''

        print('数据删除个数{}'.format(cursor1.rowcount))

        sql_string_list = []
        update_data_number = cursor1.rowcount
        update_count_number = 0
        update_count = int(update_data_number / batch_size)
        remainder_update_rows = update_data_number % batch_size

        for i in cursor1.fetchall():
            if update_count_number == update_count and remainder_update_rows !=0:
                # print(i)

                sql_string_list.append([i[0]])
                if len(sql_string_list) == remainder_update_rows:
                    cursor2.executemany(sql_string1, sql_string_list)
                    conn.commit()
                    sql_string_list = []

            if update_count_number < update_count:
                # print(i)
                print(i)
                sql_string_list.append([i[0]])
                if len(sql_string_list) == batch_size:
                    cursor2.executemany(sql_string1, sql_string_list)
                    conn.commit()
                    update_count_number += 1
                    sql_string_list = []

    def get_detail_url(self, conn):
        import redis
        pool = redis.ConnectionPool(host='127.0.0.1',
                                    # password='123456'
                                    )
        redis_pool = redis.Redis(connection_pool=pool)
        redis_pool.flushdb()
        cursor = conn.cursor()
        sql_string = '''
            SELECT
                property_id
            FROM
                tb_realtor_detail_json 
            where detail_json is NULL 
            OR is_dirty='1'
        '''
        cursor.execute(sql_string)
        for result in cursor.fetchall():
            redis_pool.lpush('realtor:property_id', 'http://{}'.format(result[0]))
        conn.commit()

    def execute_spider_close(self):
        conn = get_sql_con()
        # 将realtor_list_json表中的数据拆分开,并删除空的情况
        self.splite_list_data(conn)
        # 找到有的propertyId 并且lastUpate和address字段改变了的，这里应该使用批量更新
        self.update_detail_data(conn, 10)
        # 找到detail_page_json 表中没有的propertyId，并将它插入到该表中；
        self.insert_detail_data(conn)
        # 删除在split中没有，但是detail有的数据
        self.delete_not_exit(conn,10)
        # 将搜索条件插入到redis中
        self.get_detail_url(conn)
        conn.close()


if __name__ == "__main__":
    close_spider_process = SpiderCloseProcess()
    close_spider_process.execute_spider_close()
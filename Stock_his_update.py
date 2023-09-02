import akshare as ak
import pandas as pd
import threading
import queue
import time
from sqlalchemy import create_engine, inspect

# 创建成功和失败队列
success_queue = queue.Queue()
failure_queue = queue.Queue()

# 创建原始队列
original_queue = queue.Queue()  # 用单一队列来存储原始队列

def get_stock_time(today):
    stock_time = ak.tool_trade_date_hist_sina()
    trade_dates = stock_time.iloc[:, 0]
    trade_dates = [str(date) for date in trade_dates]
    if today not in trade_dates:
        print(f"{today} is not a trade day")
        today = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(today, "%Y-%m-%d")) - 24 * 60 * 60))
        return get_stock_time(today)
    else:
        return today

def link_stock_sql():
    # 使用 SQLAlchemy 创建数据库连接
    engine = create_engine('mysql+pymysql://root:@localhost/STOCKDB')
    return engine



def get_stock_list():
    stock_list = ak.stock_info_a_code_name()
    return stock_list

def get_stock_list_diff(stock_list, engine):
    # 获取数据库中的股票列表
    inspector = inspect(engine)
    db_stock_list = inspector.get_table_names()
    stock_list_diff = [stock for stock in stock_list['code'] if stock not in db_stock_list]
    return stock_list_diff

def get_stock_list_same(stock_list, engine):
    # 获取数据库中的股票列表
    inspector = inspect(engine)
    db_stock_list = inspector.get_table_names()
    return db_stock_list

def get_stock_list_update(engine, db_stock_list, today):
    db_stock_list_update = []
    for stock in db_stock_list:
        sql = f"SELECT MAX(stock_date) FROM `{stock}`"
        db_stock_date = pd.read_sql(sql, engine)
        db_stock_date = db_stock_date.iloc[0, 0]
        if db_stock_date != today:
            db_stock_list_update.append(stock)
    return db_stock_list_update, db_stock_date

def update_stock_data(engine, db_stock_list_update, db_stock_date, today):
    for stock in db_stock_list_update:
        stock_code = stock
        stock_history = ak.stock_zh_a_hist(symbol=stock_code, start_date=db_stock_date, end_date=today, adjust='qfq')
        if not stock_history.empty:
            stock_history = stock_history[stock_history['日期'] != db_stock_date]
            insert_stock_data(engine, stock_code, stock_history)
            print(f"{stock_code} updated to {today}")
        else:
            print(f"{stock_code} is up to date")

def process_stock():
    while True:
        try:
            stock_code = original_queue.get_nowait()
        except queue.Empty:
            break
        print(f"Fetching data for {stock_code}")
        stock_history = ak.stock_zh_a_hist(symbol=stock_code, adjust='qfq')
        if not stock_history.empty:
            insert_stock_data(engine, stock_code, stock_history)
            success_queue.put(stock_code)
        else:
            failure_queue.put(stock_code)

def insert_stock_data(engine, stock_code, stock_history):
    try:
        for index, row in stock_history.iterrows():
            date = row['日期']
            open_price = row['开盘']
            high_price = row['最高']
            low_price = row['最低']
            close_price = row['收盘']
            volume = row['成交量']
            amount = row['成交额']

            # 构建 SQL 插入语句
            sql_insert = f"INSERT INTO `{stock_code}` (stock_date, open, high, low, close, volume, amount) VALUES (%s, %s, %s, %s, %s, %s, %s)"

            # 构建 SQL 更新语句
            sql_update = f"UPDATE `{stock_code}` SET open = %s, high = %s, low = %s, close = %s, volume = %s, amount = %s WHERE stock_date = %s"

            # 检查日期是否已存在
            check_sql = f"SELECT COUNT(*) FROM `{stock_code}` WHERE stock_date = %s"
            result = engine.execute(check_sql, (date,)).fetchone()

            if result[0] == 0:
                # 如果日期不存在，执行插入操作
                engine.execute(sql_insert, (date, open_price, high_price, low_price, close_price, volume, amount))
            else:
                # 如果日期已存在，执行更新操作
                engine.execute(sql_update, (open_price, high_price, low_price, close_price, volume, amount, date))

        # 提交事务
        engine.dispose()
        print(f"Inserted/updated data for {stock_code}")
    except Exception as e:
        # 发生异常时回滚事务
        print(f"Failed to insert/update data for {stock_code}: {str(e)}")


if __name__ == '__main__':
    today = time.strftime("%Y-%m-%d", time.localtime())
    today = get_stock_time(today)
    engine = link_stock_sql()  # 创建 SQLAlchemy 数据库连接
    stock_list = get_stock_list()
    db_stock_list = get_stock_list_same(stock_list, engine)
    db_stock_list_update, db_stock_date = get_stock_list_update(engine, db_stock_list, today)

    for stock_code in db_stock_list_update:
        original_queue.put(stock_code)  # 使用 original_queue

    num_threads = 5
    threads = []

    for _ in range(num_threads):
        thread = threading.Thread(target=process_stock)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    update_stock_data(engine, db_stock_list_update, db_stock_date, today)

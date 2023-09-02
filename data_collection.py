import akshare as ak
import pymysql
import pandas as pd
import threading
import queue

# 创建成功和失败队列
success_queue = queue.Queue()
failure_queue = queue.Queue()

# 创建原始队列
original_queues = [queue.Queue() for _ in range(5)]

def get_stock_list() -> pd.DataFrame:
    stock_list = ak.stock_info_a_code_name()
    return stock_list

def get_stock_history_qfq(stock_code: str) -> pd.DataFrame:
    print(f"Fetching data for {stock_code}")
    stock_history = ak.stock_zh_a_hist(symbol=stock_code, adjust="qfq")
    stock_history = stock_history.rename(columns={"date": "stock_date"})
    return stock_history

def create_stock_table(cursor, stock_code: str):
    sql = f"CREATE TABLE IF NOT EXISTS `{stock_code}` (`stock_date` VARCHAR(10), `open` FLOAT, `high` FLOAT, `low` FLOAT, `close` FLOAT, `volume` FLOAT, `amount` FLOAT, UNIQUE(`stock_date`))"
    cursor.execute(sql)

def insert_stock_data(cursor, stock_code: str, stock_history: pd.DataFrame):
    sql = f"INSERT IGNORE INTO `{stock_code}` (`stock_date`, `open`, `high`, `low`, `close`, `volume`, `amount`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    data_to_insert = [(row[0], row[1], row[2], row[3], row[4], row[5], row[6]) for row in stock_history.itertuples(index=False)]
    cursor.executemany(sql, data_to_insert)

def process_stock(stack_number):
    try:
        # 连接数据库
        db = pymysql.connect(host='localhost', user='root', password='', database='STOCKDB')

        with db.cursor() as cursor:
            while True:
                try:
                    stock_code = original_queues[stack_number].get_nowait()
                except queue.Empty:
                    break

                print(f"Thread-{stack_number}: Fetching data for {stock_code}")

                stock_history = get_stock_history_qfq(stock_code)
                if not stock_history.empty:
                    create_stock_table(cursor, stock_code)
                    insert_stock_data(cursor, stock_code, stock_history)
                    success_queue.put(stock_code)
                else:
                    failure_queue.put(stock_code)

        db.commit()
    except Exception as e:
        # 处理异常
        print(f"Thread-{stack_number}: An error occurred: {str(e)}")
        db.rollback()
    finally:
        if db:
            db.close()

def process_failure_queue():
    while not failure_queue.empty():
        stock_code = failure_queue.get()
        process_stock(stock_code)

if __name__ == '__main__':
    # 获取股票列表
    stock_list = get_stock_list()
    stock_codes = stock_list['code'].tolist()

    # 将股票代码平均分配到原始队列
    for i, stock_code in enumerate(stock_codes):
        original_queues[i % 5].put(stock_code)

    # 多线程处理股票数据
    num_threads = 5  # 使用与原始队列数量相同的线程数
    threads = []

    for i in range(num_threads):
        thread = threading.Thread(target=process_stock, args=(i,))
        thread.start()
        threads.append(thread)

    # 等待所有线程完成
    for thread in threads:
        thread.join()

    print("Initial processing completed.")
    print("Success queue:", list(success_queue.queue))
    print("Failure queue:", list(failure_queue.queue))

    # 处理失败队列
    print("Processing failure queue...")
    process_failure_queue()

    print("All stocks processed.")
    print("Success queue:", list(success_queue.queue))
    print("Failure queue:", list(failure_queue.queue))

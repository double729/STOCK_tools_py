import akshare as ak
import pandas as pd

# 以stock_zh_a_spot_em_df.xlsx中的股票代码为列表按顺序获取股票历史数据,获取前复权和后复权数据并分别保存为以股票代码+前复权.xlsx和股票代码+后复权.xlsx的文件,保存到his文件夹中
stock_info_a_code_name_df = pd.read_excel('stock_zh_a_spot_em_df.xlsx')
# 将文件中code列转换为字符串
stock_info_a_code_name_df['code'] = stock_info_a_code_name_df['code'].astype(str)
# 将文件中code列中数据作为列表
stock_info_a_code_name_df = stock_info_a_code_name_df['code'].tolist()
# 遍历列表中的股票代码
# code从600705开始，因为600705之前的股票代码的数据已经获取过了
for code in stock_info_a_code_name_df[stock_info_a_code_name_df.index('603035'):]:

    # 如果code不足六位数，前面补0
    code = code.zfill(6)
    print(code)
    # 获取前复权和后复权数据并分别保存为以股票代码+前复权.xlsx和股票代码+后复权.xlsx的文件
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, adjust='qfq')
    stock_zh_a_hist_df.to_excel('his/stock_zh_a_hist_' + code + '_qfq.xlsx')
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, adjust='hfq')
    stock_zh_a_hist_df.to_excel('his/stock_zh_a_hist_' + code + '_hfq.xlsx')
    print('stock_zh_a_hist_' + code + '_qfq.xlsx' + ' and ' +
          'stock_zh_a_hist_' + code + '_hfq.xlsx' + ' saved' "is saved")

# 这是旧版本的代码，已经不用了
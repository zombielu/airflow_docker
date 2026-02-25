import calendar

import pandas as pd

input_file = "./data/Orders.csv"
output_file = "./data/output.csv"


# 明确哪些列是日期列
date_columns = ["date"]  # 改成你 CSV 里的列名

# 读取 CSV
df = pd.read_csv(input_file, dtype=str)  # 先都当字符串读

def replace_year_month_safe(dt, year=2026, month=2):
    if pd.isna(dt):
        return dt
    max_day = calendar.monthrange(year, month)[1]  # 当月最大天数
    day = min(dt.day, max_day)
    return dt.replace(year=year, month=month, day=day)

for col in date_columns:
    df[col] = pd.to_datetime(df[col],  errors='coerce')

    # 替换年份和月份
    df[col] = df[col].apply(
        lambda x: replace_year_month_safe(x))

    # 转回原来的字符串格式
    df[col] = df[col].dt.strftime("%Y-%m-%d")

# 保存 CSV
df.to_csv(output_file, index=False)

print("日期列替换完成，生成文件:", output_file)
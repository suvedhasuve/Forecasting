from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity="all"
import pandas as pd
from datetime import datetime
import calendar
import warnings
warnings.filterwarnings("ignore")


df_weather = pd.read_csv("FinalData_SmartMeter/weather_hourly_darksky.csv")

df_weather = df_weather[['temperature', 'time']]
df_weather.columns = ['temperature', 'DateTime']
df_weather['DateTime'] = pd.to_datetime(df_weather['DateTime'])
df_weather['year'] = df_weather['DateTime'].dt.year
df_weather['month'] = df_weather['DateTime'].dt.month
df_weather['day'] = df_weather['DateTime'].dt.day


df_weather_2013 = df_weather[df_weather.year==2013][['temperature', 'month']]
df_weather_2013 = df_weather_2013.groupby(by=['month'])['temperature'].mean().to_frame()
df_weather_2013.reset_index(level=0, inplace=True)
df_weather_2013.month = df_weather_2013.month.apply(lambda x: calendar.month_abbr[x])


df_weather_2013.to_csv("weather.csv", index=False, sep=',', encoding='utf-8')
df_household = pd.read_csv("FinalData_SmartMeter/informations_households.xls", encoding="utf-8")

df_block_0 = pd.read_csv("FinalData_SmartMeter/block_0.csv", engine='python')

df_block_0.columns = ['LCLid', 'DateTime', 'KWh']

df_block_0['DateTime'] = pd.to_datetime(df_block_0['DateTime'])
df_block_0['year'] = df_block_0['DateTime'].dt.year
df_block_0['month'] = df_block_0['DateTime'].dt.month
df_block_0['day'] = df_block_0['DateTime'].dt.day
df_block_0['hour'] = df_block_0['DateTime'].dt.hour
df_block_0['date'] = df_block_0['DateTime'].dt.date

format = '%I:%M %p'
df_block_0['time'] = df_block_0['DateTime'].dt.time.apply(lambda x: x.strftime(format))

df_block_0.loc[(df_block_0.time=='12:32 PM'), 'time'] = '12:30 PM'
df_block_0.loc[(df_block_0.time=='12:37 PM'), 'time'] = '12:30 PM'
df_block_0.loc[(df_block_0.time=='03:13 PM'), 'time'] = '03:00 PM'
df_block_0.loc[(df_block_0.time=='03:15 PM'), 'time'] = '03:00 PM'

df_household.LCLid.nunique()

df_block_0_info = df_block_0.merge(df_household, on='LCLid', how='left')

df_block_0_info.drop(['file'], axis=1, inplace=True)

df_block_0_info['Acorn'] = df_block_0_info['Acorn'].str.replace('ACORN-', "")

df_block_0_info.head(2)

df_block_0_info['stdorToU'].unique()

df_block_0_info['Acorn_grouped'].unique()

df_block_0_info = df_block_0_info[(df_block_0_info['Acorn_grouped']!='ACORN-U')]

df_block_2 = pd.read_csv("FinalData_SmartMeter/block_2.csv", engine='python')

df_block_2.columns = ['LCLid', 'DateTime', 'KWh']

df_block_2['DateTime'] = pd.to_datetime(df_block_2['DateTime'])
df_block_2['year'] = df_block_2['DateTime'].dt.year
df_block_2['month'] = df_block_2['DateTime'].dt.month
df_block_2['day'] = df_block_2['DateTime'].dt.day
df_block_2['hour'] = df_block_2['DateTime'].dt.hour
df_block_2['date'] = df_block_2['DateTime'].dt.date

df_block_2['time'] = df_block_2['DateTime'].dt.time.apply(lambda x: x.strftime(format))

df_block_2_info = df_block_2.merge(df_household, on='LCLid', how='left')

df_block_2_info.drop(['file'], axis=1, inplace=True)

df_block_2_info['Acorn'] = df_block_2_info['Acorn'].str.replace('ACORN-', "")


df_block_2_info.head(2)


df_block_2_info['Acorn_grouped'].unique()

df_block_4 = pd.read_csv("FinalData_SmartMeter/block_4.csv", engine='python')

df_block_4.columns = ['LCLid', 'DateTime', 'KWh']

df_block_4['DateTime'] = pd.to_datetime(df_block_4['DateTime'])
df_block_4['year'] = df_block_4['DateTime'].dt.year
df_block_4['month'] = df_block_4['DateTime'].dt.month
df_block_4['day'] = df_block_4['DateTime'].dt.day
df_block_4['hour'] = df_block_4['DateTime'].dt.hour
df_block_4['date'] = df_block_4['DateTime'].dt.date

df_block_4['time'] = df_block_4['DateTime'].dt.time.apply(lambda x: x.strftime(format))


df_block_4_info = df_block_4.merge(df_household, on='LCLid', how='left')


df_block_4_info.drop(['file'], axis=1, inplace=True)

df_block_4_info['Acorn'] = df_block_4_info['Acorn'].str.replace('ACORN-',"")


df_block_4_info.head(2)

df_block_4_info['Acorn_grouped'].unique()


df_block_62 = pd.read_csv("FinalData_SmartMeter/block_62.csv", engine='python')

df_block_62.columns = ['LCLid', 'DateTime', 'KWh']


# Creating date, time related columns
df_block_62['DateTime'] = pd.to_datetime(df_block_62['DateTime'])
df_block_62['year'] = df_block_62['DateTime'].dt.year
df_block_62['month'] = df_block_62['DateTime'].dt.month
df_block_62['day'] = df_block_62['DateTime'].dt.day
df_block_62['hour'] = df_block_62['DateTime'].dt.hour
df_block_62['date'] = df_block_62['DateTime'].dt.date

df_block_62['time'] = df_block_62['DateTime'].dt.time.apply(lambda x: x.strftime(format))


df_block_62_info = df_block_62.merge(df_household, on='LCLid', how='left')

df_block_62_info.drop(['file'], axis=1, inplace=True)

df_block_62_info['Acorn'] = df_block_62_info['Acorn'].str.replace('ACORN-', "")

df_block_62_info.head()


df_block_62_info['Acorn_grouped'].unique()

df_block_78 = pd.read_csv("FinalData_SmartMeter/block_78.csv", engine='python')

df_block_78.columns = ['LCLid', 'DateTime', 'KWh']

df_block_78['DateTime'] = pd.to_datetime(df_block_78['DateTime'])
df_block_78['year'] = df_block_78['DateTime'].dt.year
df_block_78['month'] = df_block_78['DateTime'].dt.month
df_block_78['day'] = df_block_78['DateTime'].dt.day
df_block_78['hour'] = df_block_78['DateTime'].dt.hour
df_block_78['date'] = df_block_78['DateTime'].dt.date


df_block_78['time'] = df_block_78['DateTime'].dt.time.apply(lambda x: x.strftime(format))


df_block_78_info = df_block_78.merge(df_household, on='LCLid', how='left')


df_block_78_info.drop(['file'], axis=1, inplace=True)

df_block_78_info['Acorn'] = df_block_78_info['Acorn'].str.replace('ACORN-', "")

df_block_78_info.head(2)

df_block_78_info['Acorn_grouped'].unique()

df_block_79 = pd.read_csv("FinalData_SmartMeter/block_79.csv", engine='python')

df_block_79.columns = ['LCLid', 'DateTime', 'KWh']

df_block_79['DateTime'] = pd.to_datetime(df_block_79['DateTime'])
df_block_79['year'] = df_block_79['DateTime'].dt.year
df_block_79['month'] = df_block_79['DateTime'].dt.month
df_block_79['day'] = df_block_79['DateTime'].dt.day
df_block_79['hour'] = df_block_79['DateTime'].dt.hour
df_block_79['date'] = df_block_79['DateTime'].dt.date
df_block_79['time'] = df_block_79['DateTime'].dt.time.apply(lambda x: x.strftime(format))


df_block_79_info = df_block_79.merge(df_household,on='LCLid',how='left')

df_block_79_info.drop(['file'], axis=1, inplace=True)
df_block_79_info['Acorn'] = df_block_79_info['Acorn'].str.replace('ACORN-', "")

df_block_79_info.Acorn_grouped.unique()

df_block_80 = pd.read_csv("FinalData_SmartMeter/block_80.csv", engine='python')
df_block_80.columns = ['LCLid', 'DateTime', 'KWh']

# Creating date, time related columns
df_block_80['DateTime'] = pd.to_datetime(df_block_80['DateTime'])
df_block_80['year'] = df_block_80['DateTime'].dt.year
df_block_80['month'] = df_block_80['DateTime'].dt.month
df_block_80['day'] = df_block_80['DateTime'].dt.day
df_block_80['hour'] = df_block_80['DateTime'].dt.hour
df_block_80['date'] = df_block_80['DateTime'].dt.date
df_block_80['time'] = df_block_80['DateTime'].dt.time.apply(lambda x: x.strftime(format))

df_block_80_info = df_block_80.merge(df_household, on='LCLid', how='left')

df_block_80_info.drop(['file'], axis=1, inplace=True)

# Cleaning: Converting 'ACORN-*' to '*'
df_block_80_info['Acorn'] = df_block_80_info['Acorn'].str.replace('ACORN-', "")

df_block_80_info['Acorn_grouped'].unique()

df_block_95 = pd.read_csv("FinalData_SmartMeter/block_95.csv", engine='python')
df_block_95.columns = ['LCLid', 'DateTime', 'KWh']
df_block_95['DateTime'] = pd.to_datetime(df_block_95['DateTime'])
df_block_95['year'] = df_block_95['DateTime'].dt.year
df_block_95['month'] = df_block_95['DateTime'].dt.month
df_block_95['day'] = df_block_95['DateTime'].dt.day
df_block_95['hour'] = df_block_95['DateTime'].dt.hour
df_block_95['date'] = df_block_95['DateTime'].dt.date

df_block_95['time'] = df_block_95['DateTime'].dt.time.apply(lambda x: x.strftime(format))


df_block_95_info = df_block_95.merge(df_household, on='LCLid', how='left')

df_block_95_info.drop(['file'], axis=1, inplace=True)

df_block_95_info['Acorn'] = df_block_95_info['Acorn'].str.replace('ACORN-', "")

df_block_95_info.head(2)

df_block_95_info['Acorn_grouped'].unique()

# Reading block_96 data
df_block_96 = pd.read_csv("FinalData_SmartMeter/block_96.csv", engine='python')
df_block_96.columns = ['LCLid', 'DateTime', 'KWh']
df_block_96['DateTime'] = pd.to_datetime(df_block_96['DateTime'])
df_block_96['year'] = df_block_96['DateTime'].dt.year
df_block_96['month'] = df_block_96['DateTime'].dt.month
df_block_96['day'] = df_block_96['DateTime'].dt.day
df_block_96['hour'] = df_block_96['DateTime'].dt.hour
df_block_96['date'] = df_block_96['DateTime'].dt.date

df_block_96['time'] = df_block_96['DateTime'].dt.time.apply(lambda x: x.strftime(format))

#%% md

df_block_96_info = df_block_96.merge(df_household, on='LCLid', how='left')

df_block_96_info.drop(['file'], axis=1, inplace=True)

df_block_96_info['Acorn'] = df_block_96_info['Acorn'].str.replace('ACORN-', "")
df_block_96_info['Acorn_grouped'].unique()
df_block_105 = pd.read_csv("FinalData_SmartMeter/block_105.csv", engine='python')
df_block_105.columns = ['LCLid', 'DateTime', 'KWh']
df_block_105['DateTime'] = pd.to_datetime(df_block_105['DateTime'])
df_block_105['year'] = df_block_105['DateTime'].dt.year
df_block_105['month'] = df_block_105['DateTime'].dt.month
df_block_105['day'] = df_block_105['DateTime'].dt.day
df_block_105['hour'] = df_block_105['DateTime'].dt.hour
df_block_105['date'] = df_block_105['DateTime'].dt.date

df_block_105['time'] = df_block_105['DateTime'].dt.time.apply(lambda x: x.strftime(format))


df_block_105_info = df_block_105.merge(df_household, on='LCLid', how='left')
df_block_105_info.drop(['file'], axis=1, inplace=True)
df_block_105_info['Acorn'] = df_block_105_info['Acorn'].str.replace('ACORN-', "")

df_block_105_info.head(2)
df_block_105_info['Acorn_grouped'].unique()

df_2 = df_block_2_info[df_block_2_info['stdorToU']=='ToU']
df_4 = df_block_4_info[df_block_4_info['stdorToU']=='ToU']
df_62 = df_block_62_info[df_block_62_info['stdorToU']=='ToU']
df_78 = df_block_78_info[df_block_78_info['stdorToU']=='ToU']
df_79 = df_block_79_info[df_block_79_info['stdorToU']=='ToU']

df_80 = df_block_80_info[df_block_80_info['stdorToU']=='ToU']


df_95 = df_block_95_info[df_block_95_info['stdorToU']=='ToU']

df_96 = df_block_96_info[df_block_96_info['stdorToU']=='ToU']

df_105 = df_block_105_info[df_block_105_info['stdorToU']=='ToU']


df_final_data = pd.concat([df_2, df_4, df_62, df_78, df_79, df_80, df_95, df_96, df_105, df_block_0_info])

df_final_data = df_final_data.rename(columns={'stdorToU' : 'Std or ToU tariff'})

df_final_data[df_final_data['Std or ToU tariff']=='ToU']['LCLid'].nunique()

df_final_data[df_final_data['Std or ToU tariff']=='Std']['LCLid'].nunique()

df_final_data['quarter'] = df_final_data['DateTime'].dt.quarter


df_final_data['quarter'] = df_final_data['year'].astype(str) +' Q'+ df_final_data['quarter'].astype(str)


df_final_data['KWh'] = df_final_data['KWh'].convert_objects(convert_numeric=True)

df_final_data.to_csv("final.csv", sep=',', index=False, encoding='utf-8')

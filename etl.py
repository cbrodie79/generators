import pandas as pd
import numpy as np

df = pd.DataFrame()
statuses = ['Operable', 'Proposed', 'Retired and Canceled']
types = ['1_Generator', '2_Wind', '3_Solar']
for i in range(6):
    for j in range(3):
        for k in range(3):
            oper_stat = statuses[j]
            type = types[k]
            if type != '1_Generator' and oper_stat == 'Proposed':
                pass
            else:
                df2 = pd.read_excel(f"3_{type}_Y{2019+i}.xlsx", header=1, sheet_name= oper_stat)
                if oper_stat == 'Operable':
                    df2 = df2[['Utility ID','Utility Name','State','Nameplate Capacity (MW)','Operating Year', 'Generator ID']]
                elif oper_stat == 'Retired and Canceled':
                    df2 = df2[['Utility ID', 'Utility Name', 'State', 'Nameplate Capacity (MW)', 'Operating Year', 'Generator ID', 'Retirement Year']]
                else:
                    df2 = df2[['Utility ID', 'Utility Name', 'State', 'Nameplate Capacity (MW)', 'Current Year', 'Generator ID']]
                    df2.rename({'Current Year':'Operating Year'})
                print(oper_stat + type + str(2019 + i))
                df2['operable_status'] = oper_stat
                df2['year'] = str(2019+i)
                df2['type'] = type[type.find('_')+1:]
                df = pd.concat([df, df2], ignore_index=True)

df.rename(columns={
    'Utility ID':'utility_id'
    ,'Utility Name':'utility_name'
    , 'State': 'state'
    , 'Nameplate Capacity (MW)': 'capacity'
    , 'Operating Year': 'operating_year'
    , 'Generator ID': 'generator_id'
    , 'Retirement Year': 'retirement_year'
}, inplace=True)
for id_col in ['utility_id', 'generator_id']:
    df[id_col] = df[id_col].astype(str)
for num_col in ['capacity', 'operating_year', 'retirement_year']:
    df[num_col] = df[num_col].replace(r'^\s*$', np.nan, regex=True)
df.to_parquet('output.parquet')
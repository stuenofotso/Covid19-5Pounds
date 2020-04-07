import datetime

import pandas as pd
import numpy as np
from fbprophet import Prophet #conda activate fbprophet_env
from functools import reduce



def forecast(inner_fit_df, period):

    inner_fit_df.columns = ['ds', 'y']

    modeler = Prophet()
    modeler.fit(inner_fit_df)

    future = modeler.make_future_dataframe(periods=period)

    forecast = modeler.predict(future)#(pd.concat([inner_fit_df[['ds']], future], axis=0))


    #print(forecast[['yhat', 'yhat_lower', 'yhat_upper']].tail())

    #final_df = pd.merge(forecast[['yhat', 'yhat_lower', 'yhat_upper', 'ds']], inner_fit_df, on='ds', how='left').reset_index()

    final_df = forecast[['yhat', 'yhat_lower', 'yhat_upper', 'ds']]

    print("forecasts on historical data ready...")
    #print(final_df.tail())

    return final_df




df = pd.read_csv("../../covid19_grouped_data.csv", sep=';').astype({'Confirmed': 'float32'}).astype({'Deaths': 'float32'}).astype({'Recovered': 'float32'})


df['Date'] =  pd.to_datetime(df['Date'],
                             format='%Y-%m-%d')



grouped_df = df[['Date', 'Confirmed','Deaths', 'Recovered']].groupby(['Date']).agg({'Confirmed':sum,'Deaths':sum,'Recovered':sum}).reset_index()


# o_df = grouped_df[['Date','Confirmed','Deaths', 'Recovered']]
# o_df['Date'] = o_df['Date']+ datetime.timedelta(days=1)
#
# grouped_df  =  pd.merge(grouped_df, o_df, on='Date', how='left').reset_index().fillna(0)
#
#
# grouped_df['Confirmed'] = grouped_df['Confirmed_x']- grouped_df['Confirmed_y']
# grouped_df['Deaths'] = grouped_df['Deaths_x']- grouped_df['Deaths_y']
# grouped_df['Recovered'] = grouped_df['Recovered_x']- grouped_df['Recovered_y']
#
# grouped_df = grouped_df[['Date', 'Confirmed','Deaths', 'Recovered']]

grouped_df.columns = ['ds', 'confirmed','deaths', 'recovered']

forecasted_confirmed_df = forecast(grouped_df[['ds', 'confirmed']], 30)
forecasted_confirmed_df.columns = ['confirmed_yhat', 'confirmed_yhat_lower', 'confirmed_yhat_upper', 'ds']


df_final = pd.merge(forecasted_confirmed_df, grouped_df, on='ds', how='left').reset_index()




forecasted_deaths_df = forecast(grouped_df[['ds', 'deaths']], 30)
forecasted_deaths_df.columns = ['deaths_yhat', 'deaths_yhat_lower', 'deaths_yhat_upper', 'ds']
df_final = pd.merge(forecasted_deaths_df, df_final, on='ds').reset_index()

df_final = df_final[['deaths_yhat', 'deaths_yhat_lower', 'deaths_yhat_upper',
                     'ds', 'confirmed_yhat', 'confirmed_yhat_lower',
                     'confirmed_yhat_upper', 'confirmed', 'deaths', 'recovered']]



forecasted_recovered_df = forecast(grouped_df[['ds', 'recovered']], 30)
forecasted_recovered_df.columns = ['recovered_yhat', 'recovered_yhat_lower', 'recovered_yhat_upper', 'ds']
df_final = pd.merge(forecasted_recovered_df, df_final, on='ds').reset_index()


#df_final = reduce(lambda left,right: pd.merge(left,right, how='inner', on='ds').reset_index(), [forecasted_confirmed_df, forecasted_deaths_df, forecasted_recovered_df]).reset_index()
#df_final = pd.merge(df_final, grouped_df, on='ds', how='left').reset_index()

print(df_final.columns)
print(df_final.tail())


df_final.to_csv("../../covid19_world_forcasted_data.csv", sep=';')



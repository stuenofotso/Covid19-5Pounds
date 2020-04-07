import pandas as pd
import numpy as np
from fbprophet import Prophet #conda activate fbprophet_env




### NOT SO COOL


df = pd.read_csv("../../covid19_grouped_data.csv", sep=';').astype({'Confirmed': 'float32'})


df['Date'] =  pd.to_datetime(df['Date'],
                             format='%Y-%m-%d')


df.columns = ['Country', 'CountryCode','ds', 'y','Deaths', 'Recovered']#['ds','y']




to_fit_df_confirmed = df.loc[df["CountryCode"]=='US'][['ds','y']]



modeler = Prophet()

modeler.fit(to_fit_df_confirmed)



forecast_hist = modeler.predict(to_fit_df_confirmed)



final_df = forecast_hist.set_index("ds").join(to_fit_df_confirmed.set_index("ds")).reset_index()

print(final_df.columns)

print("forecasts on US historical data ready...")
print(final_df[['yhat', 'yhat_lower', 'yhat_upper', 'ds', 'y']].tail(50))




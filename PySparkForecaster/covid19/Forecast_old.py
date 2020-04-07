import pandas as pd
import numpy as np
from fbprophet import Prophet #conda activate fbprophet_env







df = pd.read_csv("../../covid19_grouped_data.csv", sep=';')

print(df.columns)

print(df.head(10))

to_fit_df_confirmed = df.loc[df["Country"]=='Afghanistan'][['Date','confirmed']]

to_fit_df_confirmed.columns = ['ds','y']

print(to_fit_df_confirmed.head(5))




modeler = Prophet()

modeler.fit(to_fit_df_confirmed)



future = modeler.make_future_dataframe(periods=30)


print("future ready...")
print(future.tail())



forecast = modeler.predict(future)


print("forecasts ready...")
print(forecast[['yhat', 'yhat_lower', 'yhat_upper']].tail())



forecast_hist = modeler.predict(to_fit_df_confirmed)



final_df = forecast_hist.set_index("ds").join(df.loc[df["Country"]=='Afghanistan'].set_index("Date"))

print(final_df.columns)

print("forecasts on historical data ready...")
print(final_df[['yhat', 'yhat_lower', 'yhat_upper', 'Country', 'confirmed']].tail(50))



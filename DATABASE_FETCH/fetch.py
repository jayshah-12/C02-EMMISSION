import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import API_FETCH.functions as functions

try:
    df1 = functions.fetch_data("df1")
    df2 = functions.fetch_data("df2")
    df3 = functions.fetch_data("df3")
    emission_factors = functions.fetch_data("emission_factor")
except Exception as e:
    print(f"Error fetching data: {e}")

df2['Year'] = pd.to_datetime(df2['period']).dt.year
df2['value'] = pd.to_numeric(df2['value'], errors='coerce')
df2['value'] = df2['value'].apply(lambda x: x if x is None or x >= 0 else np.nan)
 
emission_factors_dict = dict(zip(emission_factors['fuel_type'], emission_factors['emission_factor']))
df2['emission_factor'] = df2['type-name'].map(emission_factors_dict)

df2['emission_factor'].fillna(0, inplace=True)  
df2['value'].fillna(0, inplace=True) 

df2['co2_emissions(Tons)'] = df2['value'] * df2['emission_factor']
df_aggregated = df2.groupby(['Year','respondent-name','type-name'], as_index=False)['co2_emissions(Tons)'].sum()

print(df_aggregated)
# print(df2)
# print(df_aggregated)
# print(emission_factors)
  
#########################################################################################################################################


renewables = ['Wind', 'Solar', 'Hydro']  
non_renewables = ['Coal', 'Natural Gas', 'Petroleum','Nuclear']

renewable_generation = df2[df2['type-name'].isin(renewables)]['value'].sum()

non_renewable = df2[df2['type-name'].isin(non_renewables)]
non_renewable['emission_factor'] = non_renewable['type-name'].map(emission_factors_dict)


weighted_avg_emission_factor = (non_renewable['value'] * non_renewable['emission_factor']).sum() / non_renewable['value'].sum()


co2_reduction = renewable_generation * weighted_avg_emission_factor

print(f"renewabl generation:  {renewable_generation}")
print(f"weighted average emission factor : {weighted_avg_emission_factor}")
print(f"CO2 Emissions Avoided: {co2_reduction} ")
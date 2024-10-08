import functions
import param


# data_api1 = functions.fetch_data(param.api1_url, param.api1_params, no_of_records=5000)
data_api2 = functions.fetch_data(param.api2_url, param.api2_params, no_of_records=20000) 
# data_api3 = functions.fetch_data(param.api3_url, param.api3_params, no_of_records=5000)

# print(data_api1)


# df1 = data_api1[["period",'fuel-name','state-name','value','value-units']]
df2=data_api2[['period','respondent-name','type-name','value','value-units']]
# print(data_api1.head())
# functions.mysql_connect(df1,"df1")
functions.mysql_connect(df2,"df2")
# functions.mysql_connect(data_api3,"df3")

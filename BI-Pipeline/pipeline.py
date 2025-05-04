
import pandas as pd

energy_df = pd.read_csv('global_energy_consumption.csv')
ai_content_df = pd.read_csv('Global_AI_Content_Impact_Dataset.csv')

country_replacements = {
    'USA': 'United States',
    'UK': 'United Kingdom',
    'South Korea': 'Korea, South'
}

ai_content_df['Country'] = ai_content_df['Country'].replace(country_replacements)
energy_df['Country'] = energy_df['Country'].replace(country_replacements)

country_dim = pd.DataFrame(pd.concat([energy_df['Country'], ai_content_df['Country']]).unique(), columns=['Country'])
country_dim['Country_ID'] = country_dim.index + 1
#country_dim['Continent'] = 'Unknown' Not Provided

industry_dim = pd.DataFrame(ai_content_df['Industry'].unique(), columns=['Industry'])
industry_dim['Industry_ID'] = industry_dim.index + 1

years = pd.concat([energy_df['Year'], ai_content_df['Year']]).unique()
date_dim = pd.DataFrame(years, columns=['Year'])
date_dim['Date_ID'] = date_dim.index + 1
decade = ((date_dim['Year'] // 10) * 10) % 100
date_dim['Decade'] = decade.astype(str) + 's'

regulation_dim = pd.DataFrame(ai_content_df['Regulation Status'].unique(), columns=['Status'])
regulation_dim['Regulation_ID'] = regulation_dim.index + 1

top_ai_tools_dim = pd.DataFrame(ai_content_df['Top AI Tools Used'].unique(), columns=['Tool_Name'])
top_ai_tools_dim['Tool_ID'] = top_ai_tools_dim.index + 1
#top_ai_tools_dim['Provider'] = 'Unknown' Not provided
#top_ai_tools_dim['Type'] = 'Unknown' Not provided

country_mapping = dict(zip(country_dim['Country'], country_dim['Country_ID']))
industry_mapping = dict(zip(industry_dim['Industry'], industry_dim['Industry_ID']))
date_mapping = dict(zip(date_dim['Year'], date_dim['Date_ID']))
regulation_mapping = dict(zip(regulation_dim['Status'], regulation_dim['Regulation_ID']))
tool_mapping = dict(zip(top_ai_tools_dim['Tool_Name'], top_ai_tools_dim['Tool_ID']))

fact_df = ai_content_df.copy()

fact_df['Country_ID'] = fact_df['Country'].map(country_mapping)
fact_df['Date_ID'] = fact_df['Year'].map(date_mapping)
fact_df['Industry_ID'] = fact_df['Industry'].map(industry_mapping)
fact_df['Regulation_ID'] = fact_df['Regulation Status'].map(regulation_mapping)
fact_df['Tool_ID'] = fact_df['Top AI Tools Used'].map(tool_mapping)

energy_fact = energy_df.copy()
energy_fact['Country_ID'] = energy_fact['Country'].map(country_mapping)
energy_fact['Date_ID'] = energy_fact['Year'].map(date_mapping)

fact_df = fact_df.merge(
    energy_fact[[
        'Country_ID', 'Date_ID',
        'Total Energy Consumption (TWh)',
        'Per Capita Energy Use (kWh)',
        'Renewable Energy Share (%)',
        'Fossil Fuel Dependency (%)',
        'Industrial Energy Use (%)',
        'Household Energy Use (%)',
        'Carbon Emissions (Million Tons)',
        'Energy Price Index (USD/kWh)'
    ]],
    on=['Country_ID', 'Date_ID'],
    how='left'
)

fact_df.rename(columns={
    'AI-Generated Content Volume (TBs per year)': 'AI_Yearly_Generated_Content_Volume_TB',
    'AI Adoption Rate (%)': 'AI_Adoption_Rate',
    'Job Loss Due to AI (%)': 'AI_Related_Job_Loss',
    'Revenue Increase Due to AI (%)': 'AI_Revenue_Increase',
    'Consumer Trust in AI (%)': 'Consumer_Trust_AI',
    'Market Share of AI Companies (%)': 'Market_Share_AI_Companies',
    'Total Energy Consumption (TWh)': 'Country_Energy_Consumption_TWh',
    'Per Capita Energy Use (kWh)': 'Country_EnergyUsePerCapity_kWH',
    'Renewable Energy Share (%)': 'Country_RenewableShare',
    'Fossil Fuel Dependency (%)': 'Country_Fossil_Fuel_Dependency',
    'Industrial Energy Use (%)': 'Country_Industrial_Energy_Use',
    'Household Energy Use (%)': 'Country_Household_Energy_Use',
    'Carbon Emissions (Million Tons)': 'Country_Carbon_Emissions_Mt',
    'Energy Price Index (USD/kWh)': 'Country_EnergyPriceIndex_USDkWH'
}, inplace=True)

fact_final = fact_df[[
    'Country_ID', 'Date_ID', 'Industry_ID', 'Regulation_ID', 'Tool_ID',
    'AI_Yearly_Generated_Content_Volume_TB', 'AI_Adoption_Rate', 'AI_Related_Job_Loss',
    'AI_Revenue_Increase', 'Consumer_Trust_AI', 'Market_Share_AI_Companies',
    'Country_Energy_Consumption_TWh', 'Country_EnergyUsePerCapity_kWH',
    'Country_RenewableShare', 'Country_Fossil_Fuel_Dependency', 'Country_Industrial_Energy_Use',
    'Country_Household_Energy_Use', 'Country_Carbon_Emissions_Mt', 'Country_EnergyPriceIndex_USDkWH'
]]

output_path = './output/'

country_dim.to_csv(output_path + 'dim_country.csv', index=False)
industry_dim.to_csv(output_path + 'dim_industry.csv', index=False)
date_dim.to_csv(output_path + 'dim_date.csv', index=False)
regulation_dim.to_csv(output_path + 'dim_regulation_status.csv', index=False)
top_ai_tools_dim.to_csv(output_path + 'dim_top_ai_tool.csv', index=False)
fact_final.to_csv(output_path + 'facts_table.csv', index=False)

print("Data pipeline completed")

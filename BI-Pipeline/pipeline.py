import pandas as pd

energy_df = pd.read_csv('./input/global_energy_consumption.csv')
ai_content_df = pd.read_csv('./input/Global_AI_Content_Impact_Dataset.csv')

# Data Filtering
# Energy table contains duplicate entries (multiple surveys)
# keep the row with Total Energy Consumption closest to the median for each Country-Year
def select_best_energy_row(group):
    median_val = group['Total Energy Consumption (TWh)'].median()
    idx = (group['Total Energy Consumption (TWh)'] - median_val).abs().idxmin()
    return group.loc[idx]

energy_df = (
    energy_df
      .groupby(['Country', 'Year'], group_keys=False)
      .apply(select_best_energy_row)
      .reset_index(drop=True)
)

# Country name harmonization
country_replacements = {
    'USA': 'United States',
    'UK': 'United Kingdom',
}
ai_content_df['Country'] = ai_content_df['Country'].replace(country_replacements)
energy_df['Country'] = energy_df['Country'].replace(country_replacements)

# Average duplicate rows in AI content data to ensure unique fact entries
group_keys = ['Country', 'Year', 'Industry', 'Top AI Tools Used', 'Regulation Status']
numeric_cols = ai_content_df.select_dtypes(include='number').columns

ai_content_df = (
    ai_content_df
    .groupby(group_keys, as_index=False)[numeric_cols]
    .mean()
)

# Build dimension tables
# Country Dimension
country_dim = pd.DataFrame(pd.concat([energy_df['Country'], ai_content_df['Country']]).unique(), columns=['Country'])
country_dim['Country_ID'] = country_dim.index + 1
# enrichment: add region based on country
region_map = {
    'South Korea': 'Asia',
    'China': 'Asia',
    'United States': 'North America',
    'France': 'Europe',
    'Australia':'Oceania',
    'United Kingdom': 'Europe',
    'Canada': 'North America',
    'India':'Asia',
    'Japan':'Asia',
    'Germany':'Europe',
    'Brazil': 'South America',
    'Russia': "Europe"
}
country_dim['Region'] = country_dim['Country'].map(region_map).fillna('Unknown')

# Industry Dimension
industry_dim = pd.DataFrame(ai_content_df['Industry'].unique(), columns=['Industry'])
industry_dim['Industry_ID'] = industry_dim.index + 1

# Date Dimension
years = pd.concat([energy_df['Year'], ai_content_df['Year']]).unique()
date_dim = pd.DataFrame(years, columns=['Year'])
date_dim['Date_ID'] = date_dim.index + 1
# enrichment: add decade
decade = ((date_dim['Year'] // 10) * 10)
date_dim['Decade'] = decade.astype(str) + 's'

# Regulation Dimension
regulation_dim = pd.DataFrame(ai_content_df['Regulation Status'].unique(), columns=['Status'])
regulation_dim['Regulation_ID'] = regulation_dim.index + 1

# AI Tool Dimension
top_ai_tools_dim = pd.DataFrame(ai_content_df['Top AI Tools Used'].unique(), columns=['Tool_Name'])
top_ai_tools_dim['Tool_ID'] = top_ai_tools_dim.index + 1
# enrichment: add tool type
type_map = {
    'Bard': 'Text Generation',
    'Claude': 'Text Generation',
    'ChatGPT': 'Text Generation',
    'DALL-E': 'Image Generation',
    'Stable Diffusion':'Image Generation',
    'Midjourney': 'Image Generation',
    'Synthesia':'Video Generation'
}
top_ai_tools_dim['Tool_Type'] = top_ai_tools_dim['Tool_Name'].map(type_map).fillna('Unknown')

# Create Mappings
country_mapping = dict(zip(country_dim['Country'], country_dim['Country_ID']))
industry_mapping = dict(zip(industry_dim['Industry'], industry_dim['Industry_ID']))
date_mapping = dict(zip(date_dim['Year'], date_dim['Date_ID']))
regulation_mapping = dict(zip(regulation_dim['Status'], regulation_dim['Regulation_ID']))
tool_mapping = dict(zip(top_ai_tools_dim['Tool_Name'], top_ai_tools_dim['Tool_ID']))

# Map foreign keys to fact table
fact_df = ai_content_df.copy()
fact_df['Country_ID'] = fact_df['Country'].map(country_mapping)
fact_df['Date_ID'] = fact_df['Year'].map(date_mapping)
fact_df['Industry_ID'] = fact_df['Industry'].map(industry_mapping)
fact_df['Regulation_ID'] = fact_df['Regulation Status'].map(regulation_mapping)
fact_df['Tool_ID'] = fact_df['Top AI Tools Used'].map(tool_mapping)

energy_fact = energy_df.copy()
energy_fact['Country_ID'] = energy_fact['Country'].map(country_mapping)
energy_fact['Date_ID'] = energy_fact['Year'].map(date_mapping)


# Merge energy data into fact table
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

# Rename columns
fact_df.rename(columns={
    'AI-Generated Content Volume (TBs per year)': 'AI_Yearly_Generated_Content_Volume_TB',
    'AI Adoption Rate (%)': 'AI_Adoption_Rate_pct',
    'Job Loss Due to AI (%)': 'AI_Related_Job_Loss_pct',
    'Revenue Increase Due to AI (%)': 'AI_Revenue_Increase_pct',
    'Consumer Trust in AI (%)': 'Consumer_Trust_AI_pct',
    'Market Share of AI Companies (%)': 'Market_Share_AI_Companies_pct',
    'Total Energy Consumption (TWh)': 'Country_Energy_Consumption_TWh',
    'Per Capita Energy Use (kWh)': 'Country_EnergyUsePerCapity_kWh',
    'Renewable Energy Share (%)': 'Country_RenewableShare_pct',
    'Fossil Fuel Dependency (%)': 'Country_Fossil_Fuel_Dependency_pct',
    'Industrial Energy Use (%)': 'Country_Industrial_Energy_Use_pct',
    'Household Energy Use (%)': 'Country_Household_Energy_Use_pct',
    'Carbon Emissions (Million Tons)': 'Country_Carbon_Emissions_Mt',
    'Energy Price Index (USD/kWh)': 'Country_EnergyPriceIndex_USDkWh'
}, inplace=True)

# Add default value if no data is available
fact_df.fillna(-1, inplace=True)

# Final fact table
fact_final = fact_df[[
    'Country_ID', 'Date_ID', 'Industry_ID', 'Regulation_ID', 'Tool_ID',
    'AI_Yearly_Generated_Content_Volume_TB', 'AI_Adoption_Rate_pct', 'AI_Related_Job_Loss_pct',
    'AI_Revenue_Increase_pct', 'Consumer_Trust_AI_pct', 'Market_Share_AI_Companies_pct',
    'Country_Energy_Consumption_TWh', 'Country_EnergyUsePerCapity_kWh',
    'Country_RenewableShare_pct', 'Country_Fossil_Fuel_Dependency_pct', 'Country_Industrial_Energy_Use_pct',
    'Country_Household_Energy_Use_pct', 'Country_Carbon_Emissions_Mt', 'Country_EnergyPriceIndex_USDkWh'
]]

# Sort final facts table for better readability
fact_final = fact_final.sort_values(by=['Country_ID', 'Date_ID', 'Industry_ID'])

output_path = './output/ai_energy_data.xlsx'

with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    fact_final.to_excel(writer, sheet_name='fact_table', index=False)
    country_dim.to_excel(writer, sheet_name='dim_country', index=False)
    industry_dim.to_excel(writer, sheet_name='dim_industry', index=False)
    date_dim.to_excel(writer, sheet_name='dim_date', index=False)
    regulation_dim.to_excel(writer, sheet_name='dim_regulation_status', index=False)
    top_ai_tools_dim.to_excel(writer, sheet_name='dim_top_ai_tool', index=False)

print("Data pipeline completed")

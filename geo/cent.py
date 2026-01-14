import pandas as pd
from utils.normalize import norm_county_name

main_df = pd.read_csv('data/geo/county_2025.csv')
cent_df = pd.read_csv('data/geo/County_Centroids.csv')  # Or counties_centroids.txt with sep='|'
cent_df = cent_df.rename(columns={'cfips': 'GEOID', 'latitude': 'INTPTLAT', 'longitude': 'INTPTLONG'})  # Fixed to 'cfips'
merged_df = pd.merge(main_df, cent_df, on='GEOID', how='outer')
merged_df['STATEFP'] = merged_df['GEOID'].str[:2]
merged_df['COUNTYFP'] = merged_df['GEOID'].str[2:]
merged_df['NAME_NORM'] = merged_df['NAME'].apply(norm_county_name)
merged_df.to_csv('data/geo/full_ref_with_centroids.csv', index=False)  # Use as ref
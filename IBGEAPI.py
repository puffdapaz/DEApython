import requests
import pandas as pd
import os

# API URL
link_pop_estim = 'https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2017|2019/variaveis/9324?localidades=N6[N3[11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]]'

# Request and parse JSON
response_pop_estim = requests.get(link_pop_estim)
json_pop_estim = response_pop_estim.json()

# Extract the series data
series_pop_estim = json_pop_estim[0]['resultados'][0]['series']

# Prepare data for the DataFrame
data_pop_estim = []
for item in series_pop_estim:
    row = {
        'id': item['localidade']['id'],
        'nome': item['localidade']['nome'],
        '2017': item['serie'].get('2017', None),
        '2019': item['serie'].get('2019', None)
    }
    data_pop_estim.append(row)

# Create DataFrame
df_pop_estim = pd.DataFrame(data_pop_estim)

# Optionally convert years to float if they are strings with commas
df_pop_estim['2017'] = df_pop_estim['2017'].str.replace(',', '.').astype(float)
df_pop_estim['2019'] = df_pop_estim['2019'].str.replace(',', '.').astype(float)

# Save DataFrame to Parquet file
file_path = os.path.join("data/raw", 'pop_estim.parquet')
df_pop_estim.to_parquet(file_path, index=False)
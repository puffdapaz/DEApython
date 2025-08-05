import requests
import pandas as pd

# API URL
link = 'https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2017|2019/variaveis/9324?localidades=N6[N3[11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]]'

# Request and parse JSON
response = requests.get(link)
json_data = response.json()

# Extract the series data
series_list = json_data[0]['resultados'][0]['series']

# Prepare data for the DataFrame
data = []
for item in series_list:
    row = {
        'id': item['localidade']['id'],
        'nome': item['localidade']['nome'],
        '2017': item['serie'].get('2017', None),
        '2019': item['serie'].get('2019', None)
    }
    data.append(row)

# Create DataFrame
df = pd.DataFrame(data)

# Optionally convert years to float if they are strings with commas
df['2017'] = df['2017'].str.replace(',', '.').astype(float)
df['2019'] = df['2019'].str.replace(',', '.').astype(float)

print(df.head())
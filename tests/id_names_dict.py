import requests
import pandas as pd
import os
from save import save_dataframe

# API URL
link = 'https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2017|2019/variaveis/9324?localidades=N6[N3[11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]]'

# Request and parse JSON
response = requests.get(link)
json = response.json()

# Extract the series data
series = json[0]['resultados'][0]['series']

# Prepare data for the DataFrame
data = []
for item in series:
    row = {
        'id': item['localidade']['id'],
        'nome': item['localidade']['nome']
    }
    data.append(row)

# Create DataFrame
df_municipios = pd.DataFrame(data)

# Save DataFrame 
try:
    save_dataframe(df_municipios, "API_names", directory="data/raw", file_format="csv")
except Exception as e:
    print(f"Error in API fetching: {str(e)}")
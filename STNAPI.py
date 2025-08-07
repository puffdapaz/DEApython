import basedosdados as bd
import pandas as pd
import os
import pprint
from save import save_dataframe
from dotenv import load_dotenv


# Access Key for basedosdados
load_dotenv()
bd.config.billing_project_id = os.getenv(billing_project_id)

# Search tool for basedosdados
search = bd.search("população")
pprint.pprint(search)

# PIB DataFrame
df_pib = bd.read_table(
    dataset_id="br_ibge_pib",
    table_id="municipio"
)
print(df_pib.head)

# Population DataFrame
df_pop = bd.read_table(
    dataset_id="br_ibge_populacao",
    table_id="municipio"
)
print(df_pop.head)

# Dictionary to map DataFrame variable names to filenames
raw_dfs = {
    "df_pib": df_pib,
    "df_pop": df_pop
}

# Loop over dictionary using created function to save each DataFrame
for filename, df in raw_dfs.items():
    save_dataframe(df, filename, file_format="parquet")
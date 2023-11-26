import psycopg2
import pandas as pd
import numpy as np

# Establish a connection to your PostgreSQL database
connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="password123",
    host="127.0.0.1",
    port="54310"
)

# Create a cursor object to execute SQL queries
cursor = connection.cursor()

info_query = """
INSERT INTO info_cadastro (tp_fundo, cnpj_fundo, denom_social, sit)
VALUES (%s, %s, %s, %s);
"""

time_query = """
INSERT INTO tempo (tempo_id, dt_comptc)
VALUES (%s, %s)
"""

target_query = """
INSERT INTO alvo (tempo_id, cnpj_fundo, vl_cota, vl_total, vl_patrim_liq, nr_cotst, captc_dia, resg_dia)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

columns = ["CNPJ_FUNDO", "DENOM_SOCIAL", "TP_FUNDO", "SIT"]
registration_file = "data/cad_fi.csv"
registration_df = pd.read_csv(registration_file, sep=";", encoding = "ISO-8859-1", usecols=columns)
registration_df = registration_df.drop_duplicates(subset=["CNPJ_FUNDO"])

if registration_df is None:
    exit(1)

# cursor.executemany(info_query, registration_df.values)
# connection.commit()

previous = 0

dates = pd.date_range(start="2021-11-01", end="2023-10-31", freq="M")
for date in dates:
    file_name = f"data/inf_diario_fi_{date.year}{str(date.month).rjust(2, '0')}.csv"
    df = pd.read_csv(file_name, sep=";", encoding="ISO-8859-1")

    merged_df = pd.merge(df, registration_df, how="inner", left_on="CNPJ_FUNDO", right_on="CNPJ_FUNDO")

    time = merged_df["DT_COMPTC"]
    time_values = np.column_stack((range(previous, time.shape[0] + previous), time.values))
    cursor.executemany(time_query, time_values)
    connection.commit()

    target = merged_df[["CNPJ_FUNDO", "VL_QUOTA", "VL_TOTAL", "VL_PATRIM_LIQ", "NR_COTST", "CAPTC_DIA", "RESG_DIA"]]
    target_values = np.column_stack((range(previous, time.shape[0] + previous), target.values))
    cursor.executemany(target_query, target_values)
    connection.commit()

    previous = time.shape[0] + previous

    print(f"Data from {date} uploaded!")

cursor.close()
connection.close()

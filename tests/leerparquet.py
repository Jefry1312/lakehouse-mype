import pandas as pd

df = pd.read_parquet("C:/Users/jcuadros/Documents/PROYECTOS_PY/LAST_PROYECT_AC/delta_lakehouse/bronze/mysql/solicitudes/solicitudes_20260424_v1.parquet")
print(df.head())
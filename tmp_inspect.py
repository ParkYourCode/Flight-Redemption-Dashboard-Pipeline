from pathlib import Path
import glob
import pandas as pd

p = Path('data/silver/cash')
print('exists', p.exists())
files = glob.glob(str(p / '*.parquet'))
print('files', files[:10])
if files:
    df = pd.read_parquet(files[0])
    print(df.head().to_dict(orient='records'))
    print(df.columns.tolist())
else:
    print('no parquet files')

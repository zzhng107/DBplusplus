import pandas as pd
import numpy as np





df = pd.read_csv('Demographic_Statistics_By_Zip_Code.csv')




print(df[df['PERCENT PUBLIC ASSISTANCE TOTAL']>1]['PERCENT PUBLIC ASSISTANCE TOTAL'].head(20))
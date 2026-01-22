
import pandas as pd

from backend.app.profiling.profiling import profile_dataframe

df = pd.read_csv("/Users/konansul/Desktop/github/ml-projects/02 – Logistic Regression/data/framingham.csv")

pre = profile_dataframe(df)
print(pre)
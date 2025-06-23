from tabulate import tabulate
import numpy as np
import pandas as pd

data = {
    'PHI': ['Name', 'Email', 'Phone', 'Address'],
    'PII': ['dob', 'ssn', 'Phone', 'Address']
}
df = pd.DataFrame(data)
print(df)

print(tabulate(df, headers='keys', tablefmt='fancy_grid', showindex="always"))
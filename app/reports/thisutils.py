import pandas as pd
import numpy as np

def rename_and_clean_all_columns(df: pd.DataFrame):

    for column_name in df.columns:
        if df[column_name].dtype == 'object':
            elements = df[column_name].astype(str).tolist()

            common_word = None
            for word in elements[0].split():
                if all(word in element for element in elements):
                    common_word = word
                    break

            if common_word:
                new_column_name = common_word
                df.rename(columns={column_name: new_column_name}, inplace=True)

                df[new_column_name] = df[new_column_name].str.replace(common_word, '', regex=False).str.strip()
            else:
                print(f"No se encontró una palabra común en la columna '{column_name}'.")

    return df

def split_df_by_column_ranges(df: pd.DataFrame, column_ranges, names):

  if len(column_ranges) != len(names):
    raise ValueError("The number of column ranges must match the number of names.")

  dfs = {}
  for (start, end), name in zip(column_ranges, names):
    new_df = df.iloc[:, start:end]
    dfs[name] = new_df
  return dfs

def merge_multicolumns(df: pd.DataFrame):

  levels = df.columns.nlevels

  new_cols = []

  for i in range(df.shape[1]):
    col_names = df.columns[i]

    if levels == 1:
      new_cols.append(col_names)
    else:
      new_col = ' '.join([name for name in col_names if 'Unnamed' not in name])
      new_cols.append(new_col)

  df.columns = new_cols

  return df

def create_hierarchical_rows(df: pd.DataFrame):

    original_column = df.index
    
    new_cols = {}

    for idx, row in enumerate(original_column):
        tabs = str(len(row) - len(row.lstrip(' ')))
        row = row[int(tabs):]
        for col in list(new_cols.keys()):
            new_cols[col].append(None)

        if tabs not in new_cols:
            new_cols[tabs] = ([None] * (idx)) + [row]
        else:
            new_cols[tabs][idx] = row

    df.reset_index(drop=True, inplace=True)

    hierarchy_df = pd.DataFrame(new_cols, index=None)
    for col in hierarchy_df.columns[:-1]:
        hierarchy_df[col] = hierarchy_df[col].ffill()

    reidx_hierarchy = hierarchy_df.reset_index(drop=True)

    last_col = reidx_hierarchy.columns[-1]
    reidx_hierarchy.dropna(subset=[last_col], inplace=True)

    reidx_hierarchy = rename_and_clean_all_columns(reidx_hierarchy)

    return reidx_hierarchy.merge(df.dropna(how='all'), left_index=True, right_index=True, how='inner')

def flatten_rows(df: pd.DataFrame, n: int):
    def clean_brackets(df: pd.DataFrame):
      cleaned_df = df.copy()
    
      for col in cleaned_df.columns:
          if cleaned_df[col].dtype == 'object':
              cleaned_df[col] = cleaned_df[col].apply(lambda x: x[0] if isinstance(x, list) and x else (np.nan if x == [] else x))
      
      return cleaned_df
    
    key = df.iloc[:, :n].astype(str).agg('-'.join, axis=1)
    
    flattened_rows = []

    for key_val in key.unique():
        rows = df[key == key_val]
        
        new_row = {}
        
        for col in df.columns[:n]:
            new_row[col] = rows[col].iloc[0]  # Tomar el primer valor (suponiendo que son iguales)
        
        for col in df.columns[n:]:
            new_row[col] = rows[col].dropna().unique().tolist()  # Evitar NaN y tomar valores únicos
        
        flattened_rows.append(new_row)

    flattened_df = pd.DataFrame(flattened_rows)

    flattened_df.index = key.unique()
    
    return clean_brackets(flattened_df)
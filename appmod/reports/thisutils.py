import pandas as pd
import numpy as np
from multiprocessing import Pool

def disable_func(enabled=True):
    def decorator(func):
        def wrapper(*args, **kwargs):
            if enabled:
                return func(*args, **kwargs)
            else:
                print(f"This function {func.__name__} is not enabled now.")
                return None
        return wrapper
    return decorator

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

def select_hierarchy(df: pd.DataFrame, hierarchy: str, end_hierarchy: str='eof'):
    def find_pos(substr):
        mask = df.index.str.contains(substr)
        if mask.any():
            return mask.argmax()
        else:
            return None

    idx_start = find_pos(hierarchy)
    if end_hierarchy == 'eof':
        idx_end = len(df) - 1
    else:
        idx_end = find_pos(end_hierarchy)

    print('INDEX_RANGE: ', idx_start, idx_end)

    if end_hierarchy == 'eof':
        return df.iloc[idx_start:idx_end]
    
    return df.iloc[idx_start:idx_end].iloc[:-1]

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
        return df.map(
            lambda x: x[0] if isinstance(x, list) and x else (np.nan if isinstance(x, list) and not x else x)
        )

    key = df.iloc[:, :n].astype(str).agg('-'.join, axis=1)

    first_values = df.groupby(key).first().iloc[:, :n]

    remaining_values = df.groupby(key).agg(
        {col: lambda x: pd.unique(x.dropna()).tolist() for col in df.columns[n:]}
    )

    flattened_df = pd.concat([first_values, remaining_values], axis=1)

    return clean_brackets(flattened_df)


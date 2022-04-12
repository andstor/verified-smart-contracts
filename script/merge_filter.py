
from typing import Iterable
import tqdm
import pandas as pd

def merge_filter(iterable: Iterable[pd.DataFrame], filter: callable ) -> pd.DataFrame:
    """s
    This function merges two dataframes and filters out duplicates.
    """
    levels: list[int] = []
    frames: list[pd.DataFrame] = []
    for df in iterable:
        df = filter(df)
        
        frames.append(df)
        levels.append(1)

        # Merge all df at same levels
        merge(frames, levels, filter)
    merge(frames, levels, filter, force=True)
    df = frames[0]
    df.reset_index(drop=True, inplace=True)
    return df

def merge(l: list[pd.DataFrame], levels: list, filter: callable = None, force: bool = False):
    """
    This function takes a list of dataframes and merge the dataframes at the same levels.
    Setting force to True will merge all levels.
    """
    with tqdm.tqdm(total=len(l)-1, leave=False) as pbar:
        pbar.set_description("Merging")
        while len(levels) > 1 and (force or (levels[-1] == levels[-2])):
            #print("Number of frames:", str(len(l)))
            #print("Collapsing level " + str(levels[-1]))
            r_df = l.pop()
            l_df = l.pop()

            res_df = pd.concat([l_df, r_df], ignore_index=True)
            res_df = filter(res_df) if filter is not None else res_df
            l.append(res_df)
            
            levels[-1] = levels.pop() + 1
            pbar.update(1)
        
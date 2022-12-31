#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
transform related functions

"""
from urllib import parse

# Function sourced from https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
        
def string_chunks(nested_lst):
    user_strings = []
    counts = []
    for lst in nested_lst:
        user_strings.append(','.join(lst))
        counts.append(len(lst))
    return (user_strings, counts)
    
def chunk_by_char_len(lst, char_len_limit):
    lst_copy = lst.copy()
    char_chunks = []
    i = 0
    n = 0
    
    while len(lst_copy) > 0:
        char_len = 0
        while char_len <= char_len_limit:
            if n > len(lst):
                break
            else:
                n += 1
                # print(n)
                string  = parse.quote(" OR ".join(lst_copy[0:n]))
                char_len = len(string)
        char_chunk = parse.quote(" OR ".join(lst_copy[0:n-1]))
        # print(char_chunks)
        char_chunks.append(char_chunk)
        lst_copy = lst_copy[n-1:]
        n = 0
    
    return char_chunks

def add_date(lst,datetime):
    for item in lst:
        item['snaptshot_date'] = datetime.strftime("%Y-%m-%d")

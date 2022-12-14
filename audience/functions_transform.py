# Function sourced from 
# https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks

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
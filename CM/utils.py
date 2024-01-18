''' utils.py '''

# general utility functions

def unlist(l):
    if isinstance(l, list):
        l = l[0]
    return l

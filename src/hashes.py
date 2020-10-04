FNV_prime = 1099511628211
offset_basis = 14695981039346656037
seed = 0

def fnv1a_init():
    hash = offset_basis + seed
    return hash

def fnv1a_update_str(string,hash):
    """
    Returns: The FNV-1a (alternate) hash of a given string
    """
    #FNV-1a Hash Function
    hash = offset_basis + seed
    for char in string:
      hash = hash ^ ord(char)
      hash = hash * FNV_prime

    return hash

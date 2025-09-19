def strip_string_list_comp(s):
  """Strips a string to keep only alphanumeric characters using a list comprehension."""
  return "".join(char for char in s if char.isalnum())
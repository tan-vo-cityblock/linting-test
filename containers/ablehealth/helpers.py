
def convert_snake_to_camel(column_name: str) -> str:
    words = column_name.split("_")
    return words[0] + ''.join(word.title() for word in words[1:])

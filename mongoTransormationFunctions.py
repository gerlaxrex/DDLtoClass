import os

def analyze_structure(jsonNode : dict, rootName : str) -> str:
    filename = rootName + '.java'
    filename = os.path.join('generatedFiles', filename)
    annotation_str = '\t@JsonProperty(\"{}\")\n'
    attribute_str = '\tprotected {} {};\n'
    file_str = 'public class ' + rootName + ' {\n'
    for key, value in jsonNode.items():
        if type(value) == type(dict()):
            # Subdocument recursion
            analyze_structure(value, key.capitalize())
            file_str += annotation_str.format(key)
            file_str += attribute_str.format(key.capitalize(), key)
        else:
            # String or numerical type
            file_str += annotation_str.format(key)
            file_str += attribute_str.format(value, key.lower())
    file_str += '}'

    with open(filename, 'w') as file:
        file.write(file_str)

import re
import logging
import json
from typing import Union


logging.basicConfig(filename='logging.log', level=logging.NOTSET, format='%(asctime)s - [%(levelname)s] - %(message)s', filemode='w')

type_java_mapping = {'VARCHAR2':'String',
                     'NUMBER':'Integer',
                     'DATE':'String',
                     'TIMESTAMP':'String'}


def analyze_ddl(ddl : str, from_file=False) -> dict:
    logger = logging.getLogger("analyze_ddl")
    logger.setLevel(logging.INFO)
    logger.info('Analyze_ddl started')
    if from_file:
        with open(ddl, 'r') as f:
            ddl=''.join(f.readlines())

    # Match table name

    table_name_pattern = re.compile(r'CREATE TABLE ((?P<schema>\w*)\.){0,1}(?P<table_name>\w*){1}')
    match_table_name = table_name_pattern.match(ddl)
    table_name = match_table_name.group('table_name')
    schema_name = match_table_name.group('schema')
    logger.info(f'Schema: {schema_name}, Table: {table_name}')

    # Match fields

    fields_pattern = re.compile(
        r'(\n*\s*(?P<field_name>\w*)\s*(?P<type>VARCHAR2|NUMBER|DATE|TIMESTAMP)\s*(?P<extension>\(\w*(\s*\w*){0,1}\)){0,1}\s*(?P<other>[\w\s]*)\,{0,1})')
    fields_list = []
    types_list = []
    for match in fields_pattern.finditer(ddl):
        fields_list.append(match.group('field_name'))
        types_list.append(match.group('type'))
    logger.info(f'Fields: {fields_list}')
    logger.info(f'Types: {types_list}')

    # PK match
    pk_fields = []
    pk_pattern = re.compile(r'CONSTRAINT \w+ PRIMARY KEY\s*\((?P<pks>(\s*\w+\s*,{0,1}\s*)+)\)')
    match = pk_pattern.search(ddl)
    pks = match.group('pks').split(',')
    for pk in pks:
        pk_fields.append(pk.strip())
    logger.info(f'PK Fields: {pk_fields}')
    logger.info('Analyze_ddl finished')
    return {'table_name': table_name,
            'schema_name': schema_name,
            'fields_name':fields_list,
            'fields_types': types_list,
            'pk_fields': pk_fields}

def generate_java_class(spec_dict : dict) -> None:
    class_def = ""
    filename = spec_dict['table_name'].capitalize() + 'Dto.java'
    class_def += "public class " + spec_dict['table_name'].capitalize() + "Dto {\n"

    for idx, field in enumerate(spec_dict['fields_name']):
        java_field_naming = ''.join(x.capitalize() if idx!=0 else x.lower() for idx, x in enumerate(field.split('_')))
        class_def += "\t@JsonProperty(\""+field+"\")\n"
        class_def += "\tprivate " + type_java_mapping[spec_dict['fields_types'][idx]] + " " + java_field_naming + ";\n"
    class_def += "}"

    with open(filename, 'w') as f:
        f.write(class_def)

def generate_select_query(spec_ddl: dict) -> None:
    filename = 'SelectQuery'+spec_ddl['table_name'].capitalize()+'.sql'
    sql = 'SELECT \n'
    for idx, field in enumerate(spec_ddl['fields_name']):
        if idx != len(spec_ddl['fields_name'])-1:
            sql += '\t'+field+',\n'
        else:
            sql += '\t' + field + '\n'
    sql += "FROM " + spec_ddl['table_name']
    with open(filename, 'w') as f:
        f.write(sql)

def generate_merge_query(spec_ddl: dict) -> None:
    filename = 'MergeQuery'+spec_ddl['table_name'].capitalize()+'.sql'
    sql = 'MERGE INTO ' + spec_ddl['table_name'] + ' TGT USING (\n'
    for idx, field in enumerate(spec_ddl['fields_name']):
        if idx != len(spec_ddl['fields_name']) - 1:
            sql += '\t? as ' + field + ',\n'
        else:
            sql += '\t? as ' + field + '\n'
    sql += 'FROM DUAL ) SRC ON (\n'
    for idx, field in enumerate(spec_ddl['pk_fields']):
        if idx != len(spec_ddl['pk_fields']) - 1:
            sql += '\tTGT.' + field + '=SRC.'+field+' AND\n'
        else:
            sql += '\tTGT.' + field + '=SRC.'+field+'\n'
    sql += ') WHEN MATCHED THEN \n'
    sql += 'UPDATE\nSET\n'
    for idx, field in enumerate(spec_ddl['fields_name']):
        if field not in spec_ddl['pk_fields']:
            if idx != len(spec_ddl['fields_name']) - 1:
                sql += '\tTGT.' + field + '=SRC.'+field+',\n'
            else:
                sql += '\tTGT.' + field + '=SRC.'+field+'\n'
    sql += 'WHEN NOT MATCHED THEN\nINSERT (\n'
    for idx, field in enumerate(spec_ddl['fields_name']):
        if idx != len(spec_ddl['fields_name']) - 1:
            sql += '\t' + field + ',\n'
        else:
            sql += '\t' + field + '\n'
    sql += ') VALUES (\n'
    for idx, field in enumerate(spec_ddl['fields_name']):
        if idx != len(spec_ddl['fields_name']) - 1:
            sql += '\tSRC.' + field + ',\n'
        else:
            sql += '\tSRC.' + field + '\n'
    sql += ')'
    with open(filename, 'w') as f:
        f.write(sql)



if __name__ == '__main__':
    logger_main = logging.getLogger("main")
    ddl_extractions = analyze_ddl('ddl.txt', from_file=True)
    logger_main.info(json.dumps(ddl_extractions, indent=1))

    generate_java_class(ddl_extractions)
    generate_select_query(ddl_extractions)
    generate_merge_query(ddl_extractions)
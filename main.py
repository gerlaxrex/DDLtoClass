import re
import logging
import json
from oracleTransformationFunctions import *
from mongoTransormationFunctions import analyze_structure
from typing import Union

if __name__ == '__main__':
    logger_main = logging.getLogger("main")
    ddl_extractions = analyze_ddl('ddl.txt', from_file=True)
    logger_main.info(json.dumps(ddl_extractions, indent=1))

    generate_java_class(ddl_extractions)
    generate_select_query(ddl_extractions)
    generate_merge_query(ddl_extractions)

    analyze_structure(json.loads("{\"name\":\"String\", \"surname\":\"String\", \"subdoc\":{\"A\":\"int\"}}"),'DummyClass')
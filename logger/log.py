from datetime import datetime
from .meta import time_format_names

def generate_operator_name(operator_name):
    return datetime.now().strftime(time_format_names)

def log_line(log_file, line):
    with open(log_file, "a") as file:
        file.write(line + '\n')
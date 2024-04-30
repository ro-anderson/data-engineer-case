import pandas as pd

def read_json_from_gold(filename):
    """Reads a JSON file from the gold data layer as a string."""
    path = f"../data/gold/{filename}"
    with open(path, 'r') as file:
        json_data = file.read()
    return json_data

def read_csv_from_gold(filename):
    """Reads a CSV file from the gold data layer."""
    path = f"../data/gold/{filename}"
    return pd.read_csv(path)

# Function to read data for task 0
def read_task0_data():
    return read_json_from_gold("task0_prints_last_3_weeks.json")

# Function to read data for task 1
def read_task1_data():
    return read_json_from_gold("task1_prints_with_clicked_parameter.json")

# Function to read data for task 2
def read_task2_data():
    return read_csv_from_gold("task_2_views_on_each_value_prop_last_3_weeks.csv")

# Function to read data for task 3
def read_task3_data():
    return read_csv_from_gold("task_3_clickes_on_each_value_prop_last_3_weeks.csv")

# Function to read data for tasks 4 and 5
def read_tasks4_and_5_data():
    return read_csv_from_gold("tasks_4_and_5_user_payments_summary_last_3_weeks.csv")

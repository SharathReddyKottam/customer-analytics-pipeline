import pandas as pd
import pytest
import sys
sys.path.insert(0, '.')
from etl.pipeline import transform_data

def get_sample_data():
    data = {
        'CUSTOMER_ID': [1, 2, 3, 4, 4],
        'NAME': ['Alice', None, 'Charlie', 'Dave', 'Dave'],
        'EMAIL': ['alice@email.com', 'bob@email.com', None, 'dave@email.com', 'dave@email.com'],
        'AGE': [28, 35, None, 42, 42],
        'CITY': ['New York', 'Chicago', 'Houston', 'New York', 'New York'],
        'SALARY': [75000, 55000, 62000, 48000, 48000],
        'DEPARTMENT': ['Engineering', 'Marketing', 'HR', 'Engineering', 'Engineering'],
        'JOIN_DATE': ['2022-01-15', '2021-06-20', '2020-03-10', '2023-01-05', '2023-01-05'],
        'LAST_ORDER_DATE': ['2024-03-01', '2023-12-15', '2024-02-28', '2023-10-20', '2023-10-20'],
        'TOTAL_ORDERS': [45, 12, 67, 5, 5],
        'TOTAL_SPENT': [2300.50, 890.00, 4500.75, 320.00, 320.00],
        'IS_ACTIVE': [True, True, True, False, False]
    }
    return pd.DataFrame(data)

def test_missing_names_dropped():
    df = transform_data(get_sample_data())
    assert df['name'].isnull().sum() == 0

def test_duplicates_removed():
    df = transform_data(get_sample_data())
    assert df.duplicated(subset=['email']).sum() == 0

def test_missing_age_filled():
    df = transform_data(get_sample_data())
    assert df['age'].isnull().sum() == 0

def test_churned_column_exists():
    df = transform_data(get_sample_data())
    assert 'is_churned' in df.columns
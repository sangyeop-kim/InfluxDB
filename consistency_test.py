import pandas as pd
from typing import List
import numpy as np
from collections import Counter, Hashable
from tqdm import tqdm
import plotly.express as px


def isfloat(value):
    try:
        float(value)
        return True
    except:
        return False


def isbool(value):
    try:
        return value.strip().lower() in ('true', 'false')
    except:
        return False


class DataFrameTest:
    def __init__(self, df: pd.DataFrame, tags: List, time: str):
        self.df = df
        self.tags = tags
        self.time = time
        self.fields = [col for col in df.columns if col not in tags + [time]]
        self.object_columns = [col for col in self.fields if self.df[col].dtypes == object]

        self.is_duplicate_test_ok = False
        self.is_column_name_test_ok = False
        self.is_key_validation_test_ok = False

        self.passed_fields = []
        self.hashable_test()

    def test(self):
        print(
            'unhashable columns:',
            None if self.unhashable == {} else list(self.unhashable.keys()),
        )
        self.column_name_test()
        self.duplicate_test()
        self.key_validation_test()
        self.column_data_type_test()

    def duplicate_test(self):
        print('duplicate_test: start', end='')
        if not self.is_duplicate_test_ok:
            shape = self.df.shape[0]
            self.df.drop_duplicates(
                [col for col in self.df.columns if col not in list(self.unhashable.keys())],
                inplace=True,
            )
            self.is_duplicate_test_ok = True
            print(f'\rduplicate_test: pass, row {shape} -> {self.df.shape[0]}')
        else:
            print(f'\rduplicate_test: pass ')

    def column_name_test(self):
        print('column_name_test: start', end='')
        if not self.is_column_name_test_ok:
            self.df.columns = [col.strip() for col in self.df.columns]
            self.is_column_name_test_ok = True
            print('\rcolumn_name_test: pass ')
        else:
            print('\rcolumn_name_test: pass ')

    def key_validation_test(self):
        print('key_validation_test: start', end='')
        self.count_df = self.df[self.tags + [self.time]].groupby(self.tags).count()
        count = self.count_df[self.time].values
        fig = px.histogram(pd.DataFrame({'count': count}), x='count')
        fig.show()

        if not self.is_key_validation_test_ok:
            key_shape = self.df[self.tags + [self.time]].drop_duplicates().shape[0]
            shape = self.df.shape[0]
            if key_shape == shape:
                print('\rkey_validation_test: pass ')
                self.is_key_validation_test_ok = True
            else:
                print('\rkey_validation_test: fail ')
        else:
            print('\rkey_validation_test: pass ')

    def hashable_test(self):
        self.unhashable = {}
        for col in self.object_columns:
            hashable_series = self.df[col].apply(lambda x: isinstance(x, Hashable))
            hashable_percent = np.mean(hashable_series)
            if hashable_percent < 1:
                self.unhashable[col] = hashable_series

    def column_data_type_test(self):
        print('column_data_type_test: start')

        for col in tqdm(self.object_columns):
            if col not in self.passed_fields:
                type_series = self.df[col].apply(lambda x: type(x).__name__)
                count_type = Counter(type_series)

                count_float = np.sum(
                    self.df[col][type_series == 'str'].apply(lambda x: isfloat(x))
                )
                count_bool = np.sum(
                    self.df[col][type_series == 'str'].apply(lambda x: isbool(x))
                )

                if count_float + count_bool != 0:
                    count_type['str'] -= count_float + count_bool
                    count_type['is_float'] = count_float
                    count_type['is_bool'] = count_bool

                if len(count_type.keys()) != 1:
                    print('\t', col, ':', count_type)
                else:
                    self.passed_fields.append(col)

        if self.object_columns == self.passed_fields:
            print('column_data_type_test: pass ')
        else:
            print('column_data_type_test: fail ')

    def result(self):
        print(
            f'''
unhashable columns: {None if self.unhashable == {} else list(self.unhashable.keys())},
column_name_test: {'pass' if self.is_column_name_test_ok else 'fail'}, 
duplicate_test: {'pass' if self.is_duplicate_test_ok else 'fail'},
key_validation_test: {'pass' if self.key_validation_test else 'fail'},
column_data_type_test: {'pass' if self.object_columns == self.passed_fields else 'fail'}'''
        )


if __name__ == '__main__':
    '''
    example
    '''

    temp = pd.DataFrame(
        {
            'a': [1, 2, 3, 4, 5],
            'b ': [1, 2, 2.2, 3, 4],
            'c': ['a', 'b', 'd', '1', 'b'],
            'd': ['asd', 'true', '1.2', [1, 2], 1],
        }
    )

    print(temp)
    test = DataFrameTest(temp, [], 'a')
    print('-----------------------------------------------------')
    test.test()

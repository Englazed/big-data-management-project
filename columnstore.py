from datetime import datetime, timedelta
from bisect import bisect_left, bisect_right

class ColumnStore:
    def __init__(self):
        self.columns = {}
        self.indexes = {}
        self.rows = []

    def add_column(self, name, data_type):
        if name in self.columns:
            raise ValueError(f"Column {name} already exists")
        self.columns[name] = {'data_type': data_type, 'values': []}
        self.indexes[name] = {}

    def add_row(self, row):
        if len(row) != len(self.columns):
            raise ValueError(f"Row length ({len(row)}) doesn't match column count ({len(self.columns)})")
        self.rows.append(row)
        for i, value in enumerate(row):
            column_name = list(self.columns.keys())[i]
            self.columns[column_name]['values'].append(value)
            if value not in self.indexes[column_name]:
                self.indexes[column_name][value] = []
            self.indexes[column_name][value].append(len(self.rows) - 1)

    def get_column(self, name):
        if name not in self.columns:
            raise ValueError(f"Column {name} doesn't exist")
        return self.columns[name]['values']

    def get_row(self, index):
        if index < 0 or index >= len(self.rows):
            raise ValueError(f"Row index out of range")
        return self.rows[index]
    
    def query(column_store, filters):
        # initialize result set with all rows
        result_set = set(range(len(column_store.rows)))
        
        # apply filters for each column
        for column_name, filter_value in filters.items():
            if column_name == 'date':
                if isinstance(filter_value, str):
                    filter_value = [filter_value]
                    
                row_indices = set()
                for date_filter in filter_value:
                    # Check if date_filter is a date range
                    if isinstance(date_filter, list) and len(date_filter) == 2:
                        start_date_str, end_date_str = date_filter
                        try:
                            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                            dates = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
                        except ValueError:
                            raise ValueError(f"Invalid date format in range {start_date_str} - {end_date_str}, expected format: YYYY-MM-DD")
                    else:
                        try:
                            date = datetime.strptime(date_filter, '%Y-%m-%d').date()
                            dates = [date]
                        except ValueError:
                            raise ValueError(f"Invalid date format {date_filter}, expected format: YYYY-MM-DD")
                    # Check if stored_date_str matches any date in dates, and add matching row indices to row_indices set
                    for stored_date_str in column_store.indexes[column_name].keys():
                        stored_date = datetime.strptime(stored_date_str, '%Y-%m-%d %H:%M').date()
                        if stored_date in dates:
                            row_indices.update(column_store.indexes[column_name][stored_date_str])
                result_set.intersection_update(row_indices)

            elif column_name == 'station':
                if isinstance(filter_value, str):
                    filter_value = [filter_value]
                row_indices = set()
                for station_name in filter_value:
                    row_indices.update(column_store.indexes[column_name][station_name])
                result_set.intersection_update(row_indices)
                    
            else:
                if column_name not in column_store.columns:
                    raise ValueError(f"Column {column_name} doesn't exist")
                if not isinstance(filter_value, (list, tuple)):
                    filter_value = [filter_value]
                row_indices = set()
                for value in filter_value:
                    if value not in column_store.indexes[column_name]:
                        continue
                    row_indices.update(column_store.indexes[column_name][value])
                result_set.intersection_update(row_indices)

        # Get matching rows from column_store and return them
        matching_rows = [column_store.get_row(i) for i in sorted(result_set)]
        return matching_rows




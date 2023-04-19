import csv
from columnstore import ColumnStore
from extras import write_output_file

# Define a constant for the matriculation ID to query
MATRICULATION_ID = 'U2022118J'

# Define a function to query the weather data for a given matriculation number
def query_matric(column_store, matriculation_number):
    # Determine year and location based on matriculation number E.g. A2022789B
    last_digit = int(matriculation_number[-2])
    year1 = 2000 + last_digit
    year2 = 2010 + last_digit
    location = "Changi" if int(matriculation_number[-3]) % 2 == 0 else "Paya Lebar"
    
    # Define filters based on year and location
    filters = {'date': [[f'{year1}-01-01', f'{year1}-12-31'], [f'{year2}-01-01', f'{year2}-12-31']], 'station': location}
    matching_rows = column_store.query(filters)
    
    # Group the matching rows by month
    grouped_data = {}
    for row in matching_rows:
        
        date_str = row[1]
        year, month, day = date_str.split("-")

        if month not in grouped_data:
            grouped_data[month] = []
        grouped_data[month].append(row)
        
    # Generate output rows for each category and month
    output_rows = []
    for month, month_data in grouped_data.items():
        # Max temperature
        max_temp_value = max(month_data, key=lambda x: x[3] if x[3] is not None else float('-inf'))[3]
        if max_temp_value is not None:
            max_temps = [data for data in month_data if data[3] == max_temp_value]
            for max_temp in max_temps:
                output_rows.append({
                    'Date': max_temp[1][:10],
                    'Station': max_temp[2],
                    'Category': 'Max Temperature',
                    'Value': max_temp_value
                })
            
        # Min temperature
        min_temp_value = min(month_data, key=lambda x: x[3] if x[3] is not None else float('inf'))[3]
        if min_temp_value is not None:
            min_temps = [data for data in month_data if data[3] == min_temp_value]
            for min_temp in min_temps:
                output_rows.append({
                    'Date': min_temp[1][:10],
                    'Station': min_temp[2],
                    'Category': 'Min Temperature',
                    'Value': min_temp_value
                })
            
        # Max humidity
        max_hum_value = max(month_data, key=lambda x: x[4] if x[4] is not None else float('-inf'))[4]
        if max_hum_value is not None:
            max_hums = [data for data in month_data if data[4] == max_hum_value]
            for max_hum in max_hums:
                output_rows.append({
                    'Date': max_hum[1][:10],
                    'Station': max_hum[2],
                    'Category': 'Max Humidity',
                    'Value': max_hum_value
                })
            
        # Min humidity
        min_hum_value = min(month_data, key=lambda x: x[4] if x[4] is not None else float('inf'))[4]
        if min_hum_value is not None:
            min_hums = [data for data in month_data if data[4] == min_hum_value]
            for min_hum in min_hums:
                output_rows.append({
                    'Date': min_hum[1][:10],
                    'Station': min_hum[2],
                    'Category': 'Min Humidity',
                    'Value': min_hum_value
                })
            
        # Sort output rows by date and category
    output_rows.sort(key=lambda x: (x['Date'], x['Category']))
    
    return output_rows


def main():    
    # Create a new column store object
    store = ColumnStore()
    # Add columns to the store
    store.add_column('id', int)
    store.add_column('date', str)
    store.add_column('station', str)
    store.add_column('temperature', float)
    store.add_column('humidity', float)
    
    # Read data from the CSV file and add it to the store
    with open('SingaporeWeather.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader) # skip the header row
        for row in reader:
            temp = float(row[3]) if row[3] != 'M' else None
            humidity = float(row[4]) if row[4] != 'M' else None
            row_data = [int(row[0]), row[1], row[2], temp, humidity]
            store.add_row(row_data)    

    results = query_matric(store, MATRICULATION_ID)
    write_output_file(MATRICULATION_ID, results)
    print('done!')


if __name__ == "__main__":
    main()
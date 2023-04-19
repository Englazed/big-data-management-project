import csv

def write_output_file(matriculation_ID, results):
    filename = f"ScanResult_{matriculation_ID}.csv"
    with open(filename, mode='w', newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(['Date', 'Station', 'Category', 'Value'])
        for result in results:
            date = result['Date']
            station = result['Station']
            category = result['Category']
            value = result['Value']
            if date is not None:  # omit the month if there is no data for the whole month
                writer.writerow([date, station, category, value])

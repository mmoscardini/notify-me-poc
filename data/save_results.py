import csv

def save_results(row):
    f = open('data/results.csv', 'a')

    with f:
        writer = csv.writer(f)
        # header = ["num_of_alerts", "num_of_conditions", "number_of_listing_events", "time_elapsed"]
        # writer.writerow(header)

        writer.writerow(row)
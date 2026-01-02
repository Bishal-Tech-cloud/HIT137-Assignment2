import os
import csv
import math
from collections import defaultdict

def read_all_temperature_files(folder_path="temperatures"):
    """
    Read all CSV files from the temperatures folder and combine the data.
    Returns a list of dictionaries with station data.
    """
    all_data = []
    
    # Check if folder exists
    if not os.path.exists(folder_path):
        print(f"Error: Folder '{folder_path}' not found!")
        return all_data
    
    # Get all CSV files
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"Error: No CSV files found in '{folder_path}'!")
        return all_data
    
    print(f"Found {len(csv_files)} CSV files to process...")
    
    # Read each CSV file
    for filename in csv_files:
        filepath = os.path.join(folder_path, filename)
        try:
            with open(filepath, 'r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    all_data.append(row)
            print(f"Processed: {filename}")
        except Exception as e:
            print(f"Error reading {filename}: {e}")
    
    print(f"Total records loaded: {len(all_data)}")
    return all_data

def clean_temperature_value(temp_str):
    """
    Convert temperature string to float, handling missing/NaN values.
    Returns None for invalid/missing values.
    """
    if not temp_str or temp_str.strip() == '' or temp_str.upper() == 'NAN':
        return None
    try:
        return float(temp_str)
    except ValueError:
        return None

def calculate_seasonal_averages(data):
    """
    Calculate average temperature for each season across all stations and years.
    Uses Australian seasons.
    """
    # Month to season mapping (Australian seasons)
    month_to_season = {
        'December': 'Summer',
        'January': 'Summer',
        'February': 'Summer',
        'March': 'Autumn',
        'April': 'Autumn',
        'May': 'Autumn',
        'June': 'Winter',
        'July': 'Winter',
        'August': 'Winter',
        'September': 'Spring',
        'October': 'Spring',
        'November': 'Spring'
    }
    
    # Store temperatures by season
    season_temps = defaultdict(list)
    
    for station_data in data:
        for month, season in month_to_season.items():
            temp = clean_temperature_value(station_data.get(month, ''))
            if temp is not None:  # Ignore NaN/missing values
                season_temps[season].append(temp)
    
    # Calculate averages
    seasonal_averages = {}
    for season, temps in season_temps.items():
        if temps:  # Check if we have data for this season
            avg_temp = sum(temps) / len(temps)
            seasonal_averages[season] = round(avg_temp, 1)
    
    # Order seasons chronologically
    ordered_seasons = ['Summer', 'Autumn', 'Winter', 'Spring']
    ordered_averages = {season: seasonal_averages.get(season, 0.0) for season in ordered_seasons}
    
    return ordered_averages

def calculate_temperature_ranges(data):
    """
    Find station(s) with the largest temperature range.
    Range = difference between highest and lowest temperature.
    """
    station_stats = defaultdict(lambda: {'temps': [], 'max': float('-inf'), 'min': float('inf')})
    
    # Collect all temperatures for each station
    for station_data in data:
        station_name = station_data.get('STATION_NAME', 'Unknown')
        stats = station_stats[station_name]
        
        # Check all month columns
        months = ['January', 'February', 'March', 'April', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December']
        
        for month in months:
            temp = clean_temperature_value(station_data.get(month, ''))
            if temp is not None:
                stats['temps'].append(temp)
                stats['max'] = max(stats['max'], temp)
                stats['min'] = min(stats['min'], temp)
    
    # Calculate ranges and find largest
    largest_range = 0
    stations_with_largest_range = []
    
    for station_name, stats in station_stats.items():
        if stats['temps']:  # Only if we have temperature data
            temp_range = stats['max'] - stats['min']
            stats['range'] = temp_range
            
            if temp_range > largest_range:
                largest_range = temp_range
                stations_with_largest_range = [(station_name, stats)]
            elif temp_range == largest_range:
                stations_with_largest_range.append((station_name, stats))
    
    return stations_with_largest_range, largest_range

def calculate_temperature_stability(data):
    """
    Find stations with most stable (smallest std dev) and 
    most variable (largest std dev) temperatures.
    """
    station_temps = defaultdict(list)
    
    # Collect all temperatures for each station
    for station_data in data:
        station_name = station_data.get('STATION_NAME', 'Unknown')
        
        # Check all month columns
        months = ['January', 'February', 'March', 'April', 'May', 'June',
                  'July', 'August', 'September', 'October', 'November', 'December']
        
        for month in months:
            temp = clean_temperature_value(station_data.get(month, ''))
            if temp is not None:
                station_temps[station_name].append(temp)
    
    # Calculate standard deviation for each station
    station_stddevs = {}
    
    for station_name, temps in station_temps.items():
        if len(temps) > 1:  # Need at least 2 values for std dev
            mean = sum(temps) / len(temps)
            variance = sum((x - mean) ** 2 for x in temps) / len(temps)
            stddev = math.sqrt(variance)
            station_stddevs[station_name] = round(stddev, 1)
    
    if not station_stddevs:
        return [], [], 0, 0
    
    # Find min and max std dev
    min_stddev = min(station_stddevs.values())
    max_stddev = max(station_stddevs.values())
    
    most_stable = [(station, stddev) for station, stddev in station_stddevs.items() 
                   if stddev == min_stddev]
    most_variable = [(station, stddev) for station, stddev in station_stddevs.items() 
                     if stddev == max_stddev]
    
    return most_stable, most_variable, min_stddev, max_stddev

def save_results_to_files(seasonal_averages, largest_range_stations, 
                          most_stable_stations, most_variable_stations):
    """
    Save all results to the required output files.
    """
    
    # 1. Save seasonal averages
    with open("average_temp.txt", "w") as f:
        for season, avg_temp in seasonal_averages.items():
            f.write(f"{season}: {avg_temp}°C\n")
        print("Saved seasonal averages to 'average_temp.txt'")
    
    # 2. Save largest temperature range stations
    with open("largest_temp_range_station.txt", "w") as f:
        if largest_range_stations:
            for station_name, stats in largest_range_stations:
                f.write(f"Station {station_name}: Range {stats['range']:.1f}°C ")
                f.write(f"(Max: {stats['max']:.1f}°C, Min: {stats['min']:.1f}°C)\n")
        else:
            f.write("No temperature data found.\n")
        print("Saved largest temperature range to 'largest_temp_range_station.txt'")
    
    # 3. Save temperature stability results
    with open("temperature_stability_stations.txt", "w") as f:
        f.write("Most Stable:\n")
        if most_stable_stations:
            for station_name, stddev in most_stable_stations:
                f.write(f"  Station {station_name}: StdDev {stddev}°C\n")
        else:
            f.write("  No data available\n")
        
        f.write("\nMost Variable:\n")
        if most_variable_stations:
            for station_name, stddev in most_variable_stations:
                f.write(f"  Station {station_name}: StdDev {stddev}°C\n")
        else:
            f.write("  No data available\n")
        print("Saved temperature stability results to 'temperature_stability_stations.txt'")

def main():
    """
    Main function to execute all analysis steps.
    """
    print("=" * 60)
    print("HIT137 Assignment 2 - Question 2: Temperature Analysis")
    print("=" * 60)
    
    # Step 1: Read all temperature data
    print("\nStep 1: Reading temperature data...")
    temperature_data = read_all_temperature_files()
    
    if not temperature_data:
        print("No data to process. Exiting.")
        return
    
    # Step 2: Calculate seasonal averages
    print("\nStep 2: Calculating seasonal averages...")
    seasonal_averages = calculate_seasonal_averages(temperature_data)
    
    # Display seasonal averages
    print("Seasonal Averages (across all stations and years):")
    for season, avg in seasonal_averages.items():
        print(f"  {season}: {avg}°C")
    
    # Step 3: Calculate temperature ranges
    print("\nStep 3: Calculating temperature ranges...")
    largest_range_stations, largest_range = calculate_temperature_ranges(temperature_data)
    
    # Display largest range stations
    print(f"Largest Temperature Range: {largest_range:.1f}°C")
    for station_name, stats in largest_range_stations:
        print(f"  Station: {station_name}")
        print(f"    Range: {stats['range']:.1f}°C (Max: {stats['max']:.1f}°C, Min: {stats['min']:.1f}°C)")
    
    # Step 4: Calculate temperature stability
    print("\nStep 4: Calculating temperature stability...")
    most_stable_stations, most_variable_stations, min_stddev, max_stddev = calculate_temperature_stability(temperature_data)
    
    # Display stability results
    print(f"Most Stable Stations (StdDev: {min_stddev}°C):")
    for station_name, stddev in most_stable_stations:
        print(f"  {station_name}: StdDev {stddev}°C")
    
    print(f"\nMost Variable Stations (StdDev: {max_stddev}°C):")
    for station_name, stddev in most_variable_stations:
        print(f"  {station_name}: StdDev {stddev}°C")
    
    # Step 5: Save results to files
    print("\nStep 5: Saving results to files...")
    save_results_to_files(seasonal_averages, largest_range_stations, 
                         most_stable_stations, most_variable_stations)
    
    print("\n" + "=" * 60)
    print("Analysis complete! Check the output files:")
    print("  - average_temp.txt")
    print("  - largest_temp_range_station.txt")
    print("  - temperature_stability_stations.txt")
    print("=" * 60)

if __name__ == "__main__":
    main()
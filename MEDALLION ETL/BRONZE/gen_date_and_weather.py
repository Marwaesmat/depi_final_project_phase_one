import pandas as pd
import random
from datetime import date, timedelta


# -----------------------------
# Load DIM_AIRPORT from CSV
# -----------------------------
def load_dim_airport(path="DIM_AIRPORT.csv"):
    return pd.read_csv(path)


# -----------------------------
# Generate DIM_DATE (last 7 days → today)
# -----------------------------
def generate_dim_date_dynamic():
    start_date = date.today() - timedelta(days=7)
    end_date = date.today()
    dates = pd.date_range(start=start_date, end=end_date, freq="D")

    data = []
    for i, d in enumerate(dates, start=1):
        data.append([
            i,  # date_id
            d.date(),
            d.strftime("%A"),  # day_of_week
            d.day,
            d.month,
            d.strftime("%B"),
            (d.month - 1) // 3 + 1,  # quarter
            d.year,
            d.weekday() >= 5  # is_weekend (Sat=5, Sun=6)
        ])

    return pd.DataFrame(data, columns=[
        "date_id", "calendar_date", "day_of_week", "day_of_month",
        "month_number", "month_name", "quarter", "year", "is_weekend"
    ])


# -----------------------------
# Generate DIM_WEATHER
# -----------------------------
def generate_dim_weather_dynamic(df_airport, df_date):
    weather_conditions = ["Clear", "Clouds", "Rain", "Snow", "Storm", "Fog"]
    data = []
    weather_id = 1

    for _, airport in df_airport.iterrows():
        for _, d in df_date.iterrows():
            data.append([
                weather_id,
                airport["airport_id"],
                d["date_id"],
                round(random.uniform(-10, 40), 1),  # temperature_c
                round(random.uniform(0, 50), 1),  # precipitation_mm
                round(random.uniform(0, 100), 1),  # wind_speed_kph
                random.choice(weather_conditions)
            ])
            weather_id += 1

    return pd.DataFrame(data, columns=[
        "weather_id", "airport_id", "date_id",
        "temperature_c", "precipitation_mm", "wind_speed_kph", "weather_condition"
    ])


# -----------------------------
# Run everything
# -----------------------------
if __name__ == "__main__":
    df_airport = load_dim_airport("airport.csv")
    df_date = generate_dim_date_dynamic()
    df_weather = generate_dim_weather_dynamic(df_airport, df_date)

    # Save outputs
    df_date.to_csv("date.csv", index=False)
    df_weather.to_csv("weather.csv", index=False)

    print("✅ DIM_DATE and DIM_WEATHER generated successfully!")
    print("\nDIM_DATE:\n", df_date.head())
    print("\nDIM_WEATHER sample:\n", df_weather.head())





#
# def generate_date_dimension_csv(start_date, end_date, file_name='date.csv'):
#     """
#     Generates a CSV file with date dimension data for a specified date range.
#
#     Args:
#         start_date (str): The start date of the range (format 'YYYY-MM-DD').
#         end_date (str): The end date of the range (format 'YYYY-MM-DD').
#         file_name (str): The name of the output CSV file.
#     """
#     try:
#         # Create a date range using pandas
#         date_range = pd.date_range(start=start_date, end=end_date)
#
#         # Create a dictionary to hold the data
#         data = {
#             'date_id': date_range.strftime('%Y%m%d').astype(int),  # Create an integer date ID (e.g., 20240101)
#             'calendar_date': date_range.strftime('%Y-%m-%d'),
#             'day_of_week': date_range.day_name(),
#             'day_of_month': date_range.day,
#             'month_number': date_range.month,
#             'month_name': date_range.strftime('%B'),
#             'quarter': date_range.quarter,
#             'year': date_range.year
#         }
#
#         # Create a pandas DataFrame from the dictionary
#         df = pd.DataFrame(data)
#
#         # Save the DataFrame to a CSV file
#         df.to_csv(file_name, index=False)
#
#         print(f"Successfully generated '{file_name}' with {len(df)} rows.")
#         print(f"File saved at: {os.path.abspath(file_name)}")
#
#     except Exception as e:
#         print(f"An error occurred: {e}")
#
#
# if __name__ == "__main__":
#     # Define the date range
#     start_date = '2022-01-01'
#     end_date = '2025-12-31'
#
#     # Call the function to generate the CSV file
#     generate_date_dimension_csv(start_date, end_date)
#

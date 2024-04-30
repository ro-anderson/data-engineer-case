import os
from abc import ABC, abstractmethod
import pandas as pd
from tqdm import tqdm
from loguru import logger
from collections import Counter

class DataProcessor(ABC):
    @abstractmethod
    def process_data(self):
        pass

import pandas as pd
import numpy as np
import os
from datetime import datetime


class BronzeDataProcessor(DataProcessor):
    def __init__(self, landing_dir, bronze_dir):
        self.landing_dir = landing_dir
        self.bronze_dir = bronze_dir

    def read_data(self):

        logger.info("Reading all files in the landing directory")
        files = os.listdir(self.landing_dir)
        for file in files:
            if file.endswith('.json'):
                self.process_json(file)
            elif file.endswith('.csv'):
                self.process_csv(file)

    def process_json(self, filename):
        path = os.path.join(self.landing_dir, filename)
        data = pd.read_json(path, lines=True)
        data = self.add_metadata(data, filename)
        self.save_data(data, self.convert_to_csv_filename(filename))
        logger.info(f"Processed and saved {len(data)} records from {filename} to Bronze layer")

    def process_csv(self, filename):
        path = os.path.join(self.landing_dir, filename)
        data = pd.read_csv(path)
        data = self.add_metadata(data, filename)
        self.save_data(data, filename)
        logger.info(f"Processed and saved {len(data)} records from {filename} to Bronze layer")

    def add_metadata(self, data, filename):
        data['source_file'] = filename
        data['load_timestamp'] = datetime.now().isoformat()
        return data

    def save_data(self, data, filename):
        bronze_path = os.path.join(self.bronze_dir, filename)
        data.to_csv(bronze_path, index=False)
        logger.info(f"Data saved to Bronze layer at {bronze_path}")

    def convert_to_csv_filename(self, filename):
        # Change the file extension to .csv
        return os.path.splitext(filename)[0] + '.csv'

    def process_data(self):
        logger.info("Starting processing for Bronze layer")
        self.read_data()

class SilverDataProcessor(DataProcessor):
    def __init__(self, bronze_dir, silver_dir):
        self.bronze_dir = bronze_dir
        self.silver_dir = silver_dir
        #self.gold_dir = self.silver_dir[:-6] + 'gold'

    def read_data(self):

        logger.info("Reading all files in the bronze directory")
        files = os.listdir(self.bronze_dir)
        data_frames = {}
        for file in files:
            if file.endswith('.csv'):
                # Extract base name without extension to use as dictionary key
                base_name = os.path.splitext(file)[0]
                path = os.path.join(self.bronze_dir, file)
                df = pd.read_csv(path)
                data_frames[base_name] = df
        return data_frames

    def extract_event_data(self, df):
        # Modified to only extract 'value_prop' @NOTE: need position? If so, I should edit here
        df['value_prop'] = list(tqdm(map(lambda x: eval(x).get('value_prop'), df['event_data']), total=len(df), desc="Extracting value_prop from event_data"))
        df.drop(columns=['event_data'], inplace=True)
        return df

    def process_data(self):
        logger.info(f"Processing data for Silver layer")
        data_frames = self.read_data()
        if data_frames:

            # Convert date columns to datetime and align naming
            data_frames['prints']['day'] = pd.to_datetime(data_frames['prints']['day'])
            data_frames['taps']['day'] = pd.to_datetime(data_frames['taps']['day'])
            data_frames['pays']['pay_date'] = pd.to_datetime(data_frames['pays']['pay_date'])

            # TODO: should I use this or week number? first implementation with week number

            # Other approach:
            #Find the most recent date in the prints dataset to define the last week
            #latest_date = data_frames['prints']['day'].max()
            #last_week = latest_date - pd.Timedelta(days=7)

            # Extract value_prop from event_data
            df_w_prints = self.extract_event_data(data_frames['prints'])
            df_w_taps = self.extract_event_data(data_frames['taps'])
            df_w_pays = data_frames['pays'].copy()

            # Add 'week' column indicating the week of the month
            df_w_prints['week'] = df_w_prints['day'].dt.isocalendar().week

            # task 0:
            # Step 1: Filter data for week 49
            df_week_49 = df_w_prints[df_w_prints['week'] == 49]
            
            # Step 2: Transform the DataFrame to match the JSON structure
            json_data = df_week_49.apply(self._transform_row_to_prints_format, axis=1).to_json(orient='records', lines=True)

            # Step 3: Save the JSON data to the gold layer directory
            gold_path = self.silver_dir[:-6] + 'gold'
            output_path = gold_path + '/task0_prints_last_3_weeks.json'
            with open(output_path, 'w') as file:
                file.write(json_data)

            logger.info(f"Processed and saved data to Gold layer at {output_path}")

            # Creating a 'day+value_prop' identifier for merging
            df_w_prints['day_value_prop'] = df_w_prints['day'].astype(str) + df_w_prints['value_prop']

            df_w_taps['day_value_prop'] = df_w_taps['day'].astype(str) + df_w_taps['value_prop']

            # Merge to find if prints were clicked
            merged_df = pd.merge(df_w_prints, df_w_taps, 
                                 on=['user_id', 'day'], how='left', suffixes=('_prints', '_taps'))
            merged_df['clicked'] = merged_df['day_value_prop_prints'] == merged_df['day_value_prop_taps']
            merged_df['clicked'] = merged_df['clicked'].astype(int)  # Convert boolean to 1 or 0

            # Select and rename columns as needed
            merged_df = merged_df[["day","user_id", "week", "value_prop_prints", "value_prop_taps", "clicked"]].rename(columns={"week":"week_prints"})#.rename(columns={"day":"day_prints"}, inplace=True)
            merged_df.rename(columns={"day":"day_prints"}, inplace=True)

            # Merge payment data
            merged_df = pd.merge(merged_df, df_w_pays.rename(columns={"value_prop":"value_prop_pays", "pay_date":"day_prints"}), on=['user_id','day_prints'], how='left', suffixes=('', '_pays'))
            merged_df = merged_df[['day_prints', 'user_id', 'week_prints','value_prop_prints', 'value_prop_taps', 'value_prop_pays', 'clicked', 'total']]

            # Filtering the last 4 weeks
            msk = merged_df.week_prints > 45
            merged_df = merged_df[msk]

            merged_df['week_prints'] = np.where(merged_df.week_prints == 49, 'last', 'last-3')

            # Save the final processed DataFrame if needed
            filename = 'prints_taps_and_pays_daily.csv'
            self.save_data(merged_df, filename)

            
            logger.info(f"Processed and saved {len(merged_df)} records from {filename} to Silver layer")
        else:
            logger.info(f"No data available to process.")

    def _transform_row_to_prints_format(self, row):
        return {
            "day": row['day'].strftime('%Y-%m-%d'),  # Format day as string if it's a datetime object
            "event_data": {
                "value_prop": row['value_prop']
            },
            "user_id": row['user_id']
        }

    def save_data(self, data, filename):

        silver_path = os.path.join(self.silver_dir, filename)
        data.to_csv(silver_path, index=False)
        logger.info(f"Processed and saved to Silver layer at {silver_path}")

class GoldDataProcessor(DataProcessor):
    def __init__(self, silver_dir, gold_dir):
        self.silver_dir = silver_dir
        self.gold_dir = gold_dir

    def read_data(self):
        # Path to the CSV file in the Silver layer
        logger.info("Reading all files in the Silver directory")
        filename = 'prints_taps_and_pays_daily.csv'
        path = os.path.join(self.silver_dir, filename)
        df = pd.read_csv(path)
        return df

    def process_data(self):
        logger.info("Processing data for Gold layer")
        data = self.read_data()
        if data.empty:
            logger.info(f"No data available to process.")
        else:
            logger.info("Data read successfully. Ready for further processing.")
            
            # For each print:
            #task1: ○ A field indicating if the value props were clicked or not
            df_task1 = data[['user_id', 'day_prints', 'value_prop_prints','clicked']].copy()
            out_json = self._format_task1_df_as_json(df_task1)
            self.save_task1_json(out_json,'task1_prints_with_clicked_parameter')

            # ○ task2: Each of the value props views number in the last 3 weeks prior to the print mentioned before.
            f_counter = lambda x: dict(Counter(list(x)))
            df_agg_value_prop_views = data.groupby(['user_id', 'week_prints']).agg({"value_prop_prints": f_counter}).reset_index()

            # filter only last-3 week prints
            msk = df_agg_value_prop_views.week_prints == 'last'
            self.save_data(df_agg_value_prop_views[~msk], "task_2_views_on_each_value_prop_last_3_weeks")


            # ○ task3: Number of times a user clicked on each of the value props in the last 3 weeks prior to the print mentioned before
            msk = data.week_prints == 'last'
            df_agg_clicks_for_each_value_prop = self._agg_clicks_data_for_each_user(data[~msk])

            # Save the transformed data
            self.save_data(df_agg_clicks_for_each_value_prop, "task_3_clickes_on_each_value_prop_last_3_weeks" )


            #○ task4: Number of payments made by the user for each value props in the last 3
            #weeks prior to the print mentioned before.
            #○ task5: Accumulated payments made by the user for each value props in the last 3
            #weeks prior to the print mentioned before.

            df_tmp_count_and_sum_pays = data.groupby(['user_id', 'week_prints', 'value_prop_pays']).agg({"total": set})
            df_tmp_count_and_sum_pays["count_pays"] = df_tmp_count_and_sum_pays.total.apply(lambda x: len(x))
            df_tmp_count_and_sum_pays["sum_pays"] = df_tmp_count_and_sum_pays.total.apply(lambda x: sum(x))
            df_tmp_count_and_sum_pays = df_tmp_count_and_sum_pays.reset_index()

            # Just w-3 data
            msk_last_week = df_tmp_count_and_sum_pays.week_prints == "last"
            df_tmp_count_and_sum_pays = df_tmp_count_and_sum_pays[~msk_last_week]

            # agg last 3 weeks data
            df_agg_count_and_sum_pays = self._agg_pays_data_for_each_user(df_tmp_count_and_sum_pays)

            df_agg_count_and_sum_pays.rename(columns={"count_pays":"count_of_pays_for_each_value_props","sum_pays":"sum_of_pays_for_each_value_props"}, inplace=True)

            # Save task4 and 5 file
            self.save_data(df_agg_count_and_sum_pays, "tasks_4_and_5_user_payments_summary_last_3_weeks")
            logger.info(f"Processed and saved all data to Golden layer")

    def _agg_pays_data_for_each_user(self, df):

        # Aggregate the data
        result_df = df.groupby('user_id').agg({
            'count_pays': lambda x: dict(zip(df['value_prop_pays'], x)),
            'sum_pays': lambda x: dict(zip(df['value_prop_pays'], x))
            }).reset_index()
        return result_df

    def _agg_clicks_data_for_each_user(self, df):

        # Aggregate the data
        result_df = df.groupby('user_id').agg({
            'clicked': lambda x: dict(zip(df['value_prop_prints'], x))
            }).reset_index()

        return result_df

    def _format_task1_df_as_json(self, df):

        result_json = df.rename(columns={"days_prints":"day", "value_prop_prints":"value_prop"}).to_json(orient='records', lines =True)
        return result_json

    def save_data(self, data, filename):

        # Save the processed data to the Gold layer directory
        gold_path = os.path.join(self.gold_dir, f"{filename}.csv")
        data.to_csv(gold_path, index=False)
        logger.info(f"Processed and saved data to Gold layer at {gold_path}")

    def save_task1_json(self, json_text, filename):

        # Specify the path where the JSON file will be saved        
        gold_path = os.path.join(self.gold_dir, f"{filename}.json")

        # Write the JSON string to a file in the specified Gold layer directory
        with open(gold_path, 'w') as file:
            file.write(json_text)
        
        logger.info(f"Processed and saved data to Gold layer at {gold_path}")

class DataProcessorFactory:
    def __init__(self, origin_layer_dir, destiny_layer_dir):
        self.origin_layer_dir = origin_layer_dir
        self.destiny_layer_dir = destiny_layer_dir

    def get_processor(self, layer):
        if layer == 'bronze':
            return BronzeDataProcessor(self.origin_layer_dir, self.destiny_layer_dir)
        elif layer == 'silver':
            return SilverDataProcessor(self.origin_layer_dir , self.destiny_layer_dir)
        elif layer == 'gold':
            return GoldDataProcessor(self.origin_layer_dir , self.destiny_layer_dir)
        else:
            raise ValueError("Unknown layer")



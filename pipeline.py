from etl.factory import DataProcessorFactory, BronzeDataProcessor
def main():
    landing_dir = './data/landing'
    bronze_dir = './data/bronze'
    silver_dir = './data/silver'
    gold_dir = './data/gold'
    layer = input("Enter the data layer (bronze, silver, gold): ")
    if layer == 'bronze':
        origin_layer_dir= landing_dir
        destiny_layer_dir= bronze_dir

    if layer == 'silver':
        origin_layer_dir= bronze_dir
        destiny_layer_dir= silver_dir

    if layer == 'gold':
        origin_layer_dir= silver_dir
        destiny_layer_dir= gold_dir

    processor = DataProcessorFactory(origin_layer_dir, destiny_layer_dir)
    processor =  processor.get_processor(layer)
    processor.process_data()

if __name__ == "__main__":
    main()

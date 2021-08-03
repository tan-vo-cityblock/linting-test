from cbh_setup import CityblockSparkContext
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Count the number of distinct values for a field in a BigQuery table.')
    parser.add_argument('--source_dataset', required=True)
    parser.add_argument('--source_table', required=True)
    parser.add_argument('--field', required=True)
    parser.add_argument('--destination_dataset', required=True)
    parser.add_argument('--destination_table', required=True)

    args = parser.parse_args()

    source = f'{args.source_dataset}.{args.source_table}'
    destination = f'{args.destination_dataset}.{args.destination_table}'

    cbh_sc = CityblockSparkContext(app_name='Distinct Count')
    spark = cbh_sc.session

    print(f'Reading distinct {args.field} from {source}')
    df = spark.read \
        .format('bigquery') \
        .option('table', source) \
        .load()

    distinct = df.select(args.field).distinct()
    distinct.show()

    print(f'Writing distinct values to {destination}')
    distinct.write \
        .mode('overwrite') \
        .format('bigquery') \
        .option('table', destination) \
        .save()

    print(f'Successfully wrote {distinct.count()} records to {destination}')

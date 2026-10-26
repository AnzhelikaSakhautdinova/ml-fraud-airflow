"""Create a small reproducible training fixture from full labelled competition CSV."""
import argparse
from pathlib import Path
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv')
    args = parser.parse_args()
    data = pd.read_csv(args.csv)
    dates = pd.to_datetime(data.transaction_time)
    months = dates.dt.to_period('M').astype(str)
    selected = sorted(months.unique())[-3:]
    parts = [data[months == month].sample(n=min(3000, sum(months == month)), random_state=42)
             for month in selected]
    seed = pd.concat(parts).sort_values('transaction_time')
    assert set(seed.target) == {0, 1}
    Path('data').mkdir(exist_ok=True)
    seed.to_csv('data/seed.csv', index=False)
    print(seed.groupby(seed.transaction_time.str[:7]).target.agg(['count', 'sum']))


if __name__ == '__main__':
    main()

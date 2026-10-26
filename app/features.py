"""Own shared preprocessing: cyclic time, amount, spatial deltas and categories."""
import numpy as np
import pandas as pd

CATEGORIES = ['cat_id', 'gender', 'us_state']
FEATURES = ['log_amount', 'sqrt_amount', 'hour_sin', 'hour_cos', 'weekday_sin',
            'weekday_cos', 'night', 'weekend', 'lat_delta', 'lon_delta',
            'spatial_delta', 'log_population'] + CATEGORIES


def prepare(rows):
    frame = pd.DataFrame(rows)
    required = ['transaction_time', 'amount', 'lat', 'lon', 'merchant_lat',
                'merchant_lon', 'population_city'] + CATEGORIES
    missing = set(required) - set(frame)
    if missing:
        raise ValueError(f'Missing columns: {sorted(missing)}')
    date = pd.to_datetime(frame.transaction_time, utc=True, errors='raise', format='mixed')
    if date.isna().any():
        raise ValueError('Missing time')
    numeric = frame[required[1:7]].apply(pd.to_numeric, errors='raise').astype(float)
    if not np.isfinite(numeric).all().all() or (numeric.amount < 0).any() or (numeric.population_city < 0).any():
        raise ValueError('Invalid numeric values')
    for name, bound in [('lat', 90), ('lon', 180), ('merchant_lat', 90), ('merchant_lon', 180)]:
        if (numeric[name].abs() > bound).any():
            raise ValueError(f'Invalid {name}')
    result = pd.DataFrame(index=frame.index)
    result['log_amount'] = np.log1p(numeric.amount)
    result['sqrt_amount'] = np.sqrt(numeric.amount)
    result['hour_sin'] = np.sin(2*np.pi*date.dt.hour/24)
    result['hour_cos'] = np.cos(2*np.pi*date.dt.hour/24)
    result['weekday_sin'] = np.sin(2*np.pi*date.dt.dayofweek/7)
    result['weekday_cos'] = np.cos(2*np.pi*date.dt.dayofweek/7)
    result['night'] = (date.dt.hour < 6).astype(int)
    result['weekend'] = (date.dt.dayofweek >= 5).astype(int)
    result['lat_delta'] = (numeric.lat - numeric.merchant_lat).abs()
    result['lon_delta'] = (numeric.lon - numeric.merchant_lon).abs()
    result['spatial_delta'] = np.hypot(result.lat_delta, result.lon_delta)
    result['log_population'] = np.log1p(numeric.population_city)
    for name in CATEGORIES:
        result[name] = frame[name].fillna('missing').astype(str).str.strip().replace('', 'missing')
    return result[FEATURES]

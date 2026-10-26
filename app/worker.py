"""Score with the version identified by the atomic registry pointer."""
import json
import logging
from catboost import CatBoostClassifier
from app.features import FEATURES, prepare
from app.broker import producer, publish, run
from pipeline import storage


class Scorer:
    def __init__(self):
        self.version = None
        self.model = None

    def reload(self):
        pointer = storage.active()
        if pointer is None:
            raise RuntimeError('Model not published yet')
        version = pointer['version']
        if version != self.version:
            directory = storage.ROOT / 'models' / version
            metadata = json.loads((directory / 'metadata.json').read_text())
            if metadata['features'] != FEATURES or metadata['sha256'] != storage.digest(directory / 'model.cbm'):
                raise RuntimeError('Model checksum/feature contract mismatch')
            model = CatBoostClassifier()
            model.load_model(str(directory / 'model.cbm'))
            if model.feature_names_ != FEATURES:
                raise RuntimeError('Wrong model features')
            self.model, self.version = model, version
            logging.info('Loaded model %s', version)

    def score(self, value):
        self.reload()
        if not isinstance(value, dict) or not isinstance(value.get('transaction_id'), str) or not value['transaction_id'].strip():
            raise ValueError('Expected string transaction_id')
        payload = value.get('data', value)
        score = float(self.model.predict_proba(prepare([payload]), task_type='CPU', thread_count=1)[0, 1])
        return dict(transaction_id=value['transaction_id'], score=score,
                    fraud_flag=int(score >= 0.5), model_version=self.version)


def main():
    scorer, output = Scorer(), producer()
    def handle(value):
        result = scorer.score(value)
        publish(output, 'scores', result, result['transaction_id'])
    run('transactions', 'hw3-scorer', handle)


if __name__ == '__main__':
    main()

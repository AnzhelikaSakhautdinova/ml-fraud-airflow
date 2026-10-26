import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import pandas as pd

from app.features import prepare, FEATURES
from app.worker import Scorer
from pipeline import storage
from pipeline.months import shift
from pipeline.tasks import supply_month, prepare_window, train_candidate, evaluate_candidate, publish_candidate
from pipeline.quality import check_data, render_report


class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.seed = pd.read_csv('data/seed.csv')

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root_patch = patch.object(storage, 'ROOT', Path(self.temporary.name))
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        with storage.connect() as connection:
            for index, row in self.seed.iterrows():
                payload = row.to_dict()
                connection.execute('INSERT INTO transactions VALUES (?,?,?)',
                                   (str(index), payload['transaction_time'][:7], json.dumps(payload)))

    def initial(self):
        plan = prepare_window()
        publish_candidate(evaluate_candidate(train_candidate(plan)))
        return plan

    def test_feature_contract_and_no_target_leak(self):
        first = prepare(self.seed.head(5))
        self.assertEqual(list(first), FEATURES)
        pd.testing.assert_frame_equal(first, prepare(self.seed.head(5).assign(target=99)))
        with self.assertRaises(ValueError):
            prepare(self.seed.head(1).assign(amount=-1))

    def test_time_split_and_frozen_plan(self):
        plan = prepare_window()
        directory = Path(plan['directory'])
        train = pd.read_csv(directory / 'train.csv')
        test = pd.read_csv(directory / 'test.csv')
        self.assertLess(train.transaction_time.max(), test.transaction_time.min())
        self.assertEqual(len(plan['months']), 3)
        self.assertEqual(prepare_window(), plan)
        self.assertIsNone(storage.active())

    def test_supply_retry_and_window(self):
        self.initial()
        pointer = storage.active()
        with storage.connect() as connection:
            connection.execute('INSERT INTO transactions VALUES (?,?,?)', ('obsolete', '1900-01', json.dumps(self.seed.iloc[0].to_dict())))
        first, second = supply_month(), supply_month()
        self.assertEqual(first['month'], second['month'])
        self.assertEqual(second['status'], 'already_supplied')
        self.assertEqual(storage.active(), pointer)
        with storage.connect() as connection:
            count = connection.execute('SELECT count() FROM transactions WHERE month=?', (first['month'],)).fetchone()[0]
            self.assertEqual(connection.execute("SELECT count() FROM transactions WHERE id='obsolete'").fetchone()[0], 0)
        self.assertEqual(count, 3000)
        plan = prepare_window()
        self.assertEqual(plan['months'][0], shift(pointer['processed_month'], -1))
        publish_candidate(evaluate_candidate(train_candidate(plan)))
        supply_month()
        next_plan = prepare_window()
        self.assertEqual(next_plan['month'], shift(plan['month']))
        prepare(pd.read_csv(Path(next_plan['directory']) / 'train.csv'))

    def test_rejection_preserves_model(self):
        self.initial()
        old = storage.active()
        supply_month()
        plan = train_candidate(prepare_window())
        # Drive the actual evaluator's quality gate with a strictly worse candidate.
        with patch('pipeline.tasks.average_precision_score', side_effect=[0.0, 1.0]):
            evaluate_candidate(plan)
        report = json.loads((Path(plan['directory']) / 'evaluation.json').read_text())
        self.assertFalse(report['accepted'])
        result = publish_candidate(plan)
        self.assertEqual(result['version'], old['version'])
        self.assertEqual(result['processed_month'], plan['month'])
        self.assertEqual(publish_candidate(plan), result)

    def test_hot_reload_and_atomic_checkpoint_retry(self):
        self.initial()
        scorer = Scorer()
        message = {'transaction_id': 'probe', 'data': self.seed.iloc[0].to_dict()}
        before = scorer.score(message)
        supply_month()
        plan = prepare_window()
        # Identical incumbent is a legitimate equal-quality candidate, accepted by policy.
        import shutil
        shutil.copyfile(storage.ROOT / 'models' / before['model_version'] / 'model.cbm', Path(plan['directory']) / 'candidate.cbm')
        evaluate_candidate(plan)
        old = storage.active()
        original = storage.atomic_json
        def fail_pointer(path, value):
            if Path(path).name == 'active.json':
                raise OSError('simulated crash before pointer swap')
            return original(path, value)
        with patch.object(storage, 'atomic_json', side_effect=fail_pointer):
            with self.assertRaises(OSError):
                publish_candidate(plan)
        self.assertEqual(storage.active(), old)
        after_pointer = publish_candidate(plan)
        after = scorer.score(message)
        self.assertNotEqual(before['model_version'], after['model_version'])
        self.assertEqual(after['model_version'], after_pointer['version'])
        self.assertAlmostEqual(before['score'], after['score'])

    def test_dq_reports_failure_and_success(self):
        good = check_data()
        self.assertTrue(good['passed'])
        render_report(good)
        with storage.connect() as connection:
            payload = self.seed.iloc[0].to_dict()
            payload['amount'] = -1
            connection.execute('UPDATE transactions SET payload=? WHERE id=?', (json.dumps(payload), '0'))
        bad = check_data()
        self.assertFalse(bad['rules']['nonnegative_amount_population'])
        with self.assertRaises(ValueError):
            render_report(bad)
        self.assertIn('FAIL', (storage.ROOT / 'reports/dq.html').read_text())

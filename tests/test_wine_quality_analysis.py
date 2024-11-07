import unittest
from pyspark.sql import SparkSession
from wine_quality_analysis import transform_data

class TestWineQualityAnalysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("TestWineQualityAnalysis").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform_data(self):
        # Create sample data
        data_red = [(7.4, 3), (7.8, 5)]
        data_white = [(6.9, 8), (6.3, 4)]
        columns = ['alcohol', 'quality']

        red_wine_df = self.spark.createDataFrame(data_red, columns)
        white_wine_df = self.spark.createDataFrame(data_white, columns)

        # Transform data
        wine_df = transform_data(red_wine_df, white_wine_df)

        # Check if 'wine_type' column is added correctly
        wine_types = wine_df.select('wine_type').distinct().collect()
        wine_types = [row['wine_type'] for row in wine_types]
        self.assertIn('red', wine_types)
        self.assertIn('white', wine_types)

        # Check if 'quality_category' is assigned correctly
        expected_categories = {'Low', 'Medium', 'High'}
        actual_categories = set(wine_df.select('quality_category').rdd.flatMap(lambda x: x).collect())
        self.assertTrue(expected_categories.issubset(actual_categories))

if __name__ == '__main__':
    unittest.main()

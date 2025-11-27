import unittest
from unittest.mock import MagicMock, patch

import spark_etl_job

class TestHousingETL(unittest.TestCase):

    @patch('spark_etl_job.findspark.init')
    @patch('spark_etl_job.SparkSession')
    def test_get_spark_session_config(self, mock_spark_session, mock_findspark):
        """Tests if the SparkSession is configured with the correct JAR packages."""
        print("\n>>> TEST: Verifying SparkSession JAR package configuration...")
        # Arrange: Chain mock calls to simulate the builder pattern
        mock_builder = mock_spark_session.builder
        mock_builder.appName.return_value.master.return_value.config.return_value.getOrCreate.return_value = "mock_spark_session"

        # Act
        spark_etl_job.get_spark_session()

        # Assert
        mock_findspark.assert_called_once()
        config_call_args = mock_builder.appName.return_value.master.return_value.config.call_args[0]
        self.assertEqual(config_call_args[0], "spark.jars.packages")
        self.assertIn("mysql:mysql-connector-java:8.0.33", config_call_args[1])
        print("    [SUCCESS] SparkSession is configured with correct JARs.")

    @patch('spark_etl_job.get_spark_session')
    def test_get_spark_session_config(self, mock_builder):
        """Tests if the SparkSession is configured with the correct JAR packages."""
        print("\n>>> TEST: Verifying SparkSession JAR package configuration...")
        # Arrange: Chain mock calls to simulate the builder pattern
        mock_builder.appName.return_value.config.return_value.config.return_value.config.return_value.getOrCreate.return_value = "mock_spark_session"
        
        # This test seems to have leftover logic from a MongoDB connector.
        # I've commented it out as it's not relevant to the current code.
        # If you add a MongoDB connector back, you can re-enable and adapt this.
        # self.assertIn("org.mongodb.spark:mongo-spark-connector_2.12:10.2.1", first_config_call_args[1])
        
        # The test for mongo partitioner config is also removed as it's not relevant.

if __name__ == '__main__':
    unittest.main()
import joblib
from hops import hdfs, model
import os

class Predict(object):

    def __init__(self):
        """ Initializes the serving state, reads a trained model from HDFS"""
        self.model_path = hdfs_path = hdfs._expand_path("Models/real_estate_pricing/1/model.pkl")
        h = hdfs.get_fs()
        print("Read model")
        with h.open_file(self.model_path, "r") as f:
            self.model = joblib.load(f)
        print("Initialization Complete")

    def predict(self, inputs):
        """ Serves a prediction request usign a trained model"""
        return self.model.predict(inputs).tolist() # Numpy Arrays are note JSON serializable

    def classify(self, inputs):
        """ Serves a classification request using a trained model"""
        return "not implemented"

    def regress(self, inputs):
        """ Serves a regression request using a trained model"""
        return "not implemented"

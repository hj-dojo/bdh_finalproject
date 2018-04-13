import classifier
import numpy
from sklearn.model_selection import KFold, train_test_split


def getKFoldMetrics(X, Y, k=5):
	metrics = numpy.zeros(5)
	validator = KFold(n_splits=k)
	for trainingIndex, testingIndex in validator.split(X):
		predictions = classifier.predict(X[trainingIndex], Y[trainingIndex], X[testingIndex])
		predictions = classifier.applyPredictionThresholding(predictions)
		metrics += classifier.getMetrics(Y[testingIndex], predictions)

	metrics /= k # average results from each fold
	return metrics
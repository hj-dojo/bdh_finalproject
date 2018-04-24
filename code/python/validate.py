import classifier
import numpy
from sklearn.model_selection import KFold, train_test_split, StratifiedKFold

RANDOM_STATE = 545510477

def getKFoldMetrics(X, Y, k=5):
	metrics = numpy.zeros(5)
	validator = KFold(n_splits=k, random_state=RANDOM_STATE)
	for trainingIndex, testingIndex in validator.split(X):
		predictions, probabilities = classifier.predict(X[trainingIndex], Y[trainingIndex], X[testingIndex])
		metrics += classifier.getMetrics(Y[testingIndex], predictions, probabilities[:,1])

	metrics /= k # average results from each fold
	return metrics

def getStratifiedKFoldMetrics(X, Y, k=5):
	metrics = numpy.zeros(5)

	validator = StratifiedKFold(n_splits=k, shuffle=True, random_state=RANDOM_STATE)
	for trainingIndex, testingIndex in validator.split(X, Y):
		predictions, probabilities = classifier.predict(X[trainingIndex], Y[trainingIndex], X[testingIndex])
		metrics += classifier.getMetrics(Y[testingIndex], predictions, probabilities[:,1])

	metrics /= k # average results from each fold
	return metrics
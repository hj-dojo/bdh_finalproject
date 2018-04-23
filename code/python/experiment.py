import classifier
import validate

import os
import numpy
import glob
import re
import argparse
import matplotlib.pyplot as plt
from sklearn.datasets import load_svmlight_file
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import normalize
from sklearn.preprocessing import StandardScaler


run_cross_validation = True


def get_data_from_svmlight(filepath):
	filename = glob.glob(filepath+"/*.libsvm").pop()
	svmdata = load_svmlight_file(filename, n_features=12)
	x = svmdata[0]
	y = svmdata[1]
	return x, y


def printMetrics(metrics):
	print('  Accuracy:  ', metrics[0])
	print('  AUC:       ', metrics[1])
	print('  Precision: ', metrics[2])
	print('  Recall:    ', metrics[3])
	print('  F1-Score:  ', metrics[4], '\n')


if __name__ == '__main__':

	parser = argparse.ArgumentParser(prog="python3 experiment.py",
									 description="Retrieve classification metrics")
	parser.add_argument('--path', '-p', default='../output/svmoutput')
	parser.add_argument('--obsWinHrs', '-o', default=24, type=int)
	parsed_args = parser.parse_args()
	filedir = parsed_args.path
	obsWinHrs = parsed_args.obsWinHrs

	# Create charts directory if it doesn't exist
	os.makedirs('charts', exist_ok=True)

	windows = []
	scores = []
	roc_curves = [] # stores the different ROC metrics for composite chart

	# Traverse directory containing feature files in svmlight format
	for filepath in sorted(glob.glob(filedir + "/*")):
		w = int(re.findall('\d+', filepath)[-1])
		windows.append(w)

		X, Y = get_data_from_svmlight(filepath)
		X = StandardScaler(with_std=True, with_mean=False).fit_transform(X)  # normalize feature set

		xtrain, xtest, ytrain, ytest = train_test_split(X, Y, test_size=0.25, random_state=numpy.random.RandomState(0))
		ypred, yprob = classifier.predict(xtrain, ytrain, xtest)
		metrics = classifier.getMetrics(ytest, ypred, yprob[:,1])

		print('{0}-Hour Window'.format(w))
		print('  Case:      ', Y[Y == 1].shape[0])
		print('  Control:   ', Y[Y == 0].shape[0])
		printMetrics(metrics)
		scores.append(metrics)

		classifier.getPrecisionRecallCurve(ytest, yprob[:,1], 'charts/precision-recall-{0}hr.png'.format(w), w)
		roc_curves.append(classifier.getROCCurve(ytest, yprob[:,1], metrics[1], 'charts/roc-{0}hr.png'.format(w), w))

		if run_cross_validation:
			print('K-Fold Cross-Validation Metrics (pred window: {0}hr)'.format(w))
			printMetrics(validate.getKFoldMetrics(X, Y, k=5))

			print('Stratified K-Fold Cross-Validation Metrics (pred window: {0}hr)'.format(w))
			printMetrics(validate.getStratifiedKFoldMetrics(X, Y, k=5))

	scores = numpy.asarray(scores)

	# all metrics for experiment
	plt.figure()
	plt.plot(scores[:,1], label='AUROC')
	plt.plot(scores[:,2], label='Precision')
	plt.plot(scores[:,3], label='Recall')
	plt.plot(scores[:,4], label='F1 Score')
	plt.legend()
	plt.xlabel('Prediction Window (Hours)')
	plt.ylabel('Score')
	plt.title('All Metrics: {0}-Hr Observation Window'.format(obsWinHrs))
	plt.xticks([ i for i in range(5) ], windows)
	plt.savefig('charts/all-metrics.png')
	plt.close()

	# consolidated ROC curves (probability-based)
	plt.figure()
	for w in range(len(windows)):
		plt.plot(roc_curves[w][0], roc_curves[w][1], label='{0}-Hour'.format(windows[w]))
	plt.plot([0,1],[0,1], 'k--')
	plt.xlim([0.,1.])
	plt.ylim([0.,1.05])
	plt.xlabel('False Positive Rate')
	plt.ylabel('True Positive Rate')
	plt.title('Receiver Operating Characteristic: {0}-Hr Observation Window'.format(obsWinHrs))
	plt.legend(loc='lower right')
	plt.savefig('charts/all-roc-curves.png')
	plt.close()



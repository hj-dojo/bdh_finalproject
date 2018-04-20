import classifier
import validate

import numpy
import matplotlib.pyplot as plt
from sklearn.datasets import load_svmlight_file
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import normalize
from sklearn.preprocessing import StandardScaler


run_cross_validation = False


def get_data_from_svmlight(window):
	svmdata = load_svmlight_file('../output/svmoutput/features{0}hour/features.libsvm'.format(window), n_features=12)
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
	
	if run_cross_validation:
		X, Y = get_data_from_svmlight('4')
		X = StandardScaler(with_std=True, with_mean=False).fit_transform(X) # normalize feature set

		print('K-Fold Cross-Validation Metrics')
		printMetrics(validate.getKFoldMetrics(X, Y, k=5))

		print('Stratified K-Fold Cross-Validation Metrics')
		printMetrics(validate.getStratifiedKFoldMetrics(X, Y, k=5))

	windows = [1,2,4,6,8]
	scores = []
	roc_curves = [] # stores the different ROC metrics for composite chart

	for w in windows:
		X, Y = get_data_from_svmlight(str(w))
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
	plt.title('Receiver Operating Characteristic')
	plt.legend(loc='lower right')
	plt.savefig('charts/all-roc-curves.png')
	plt.close()



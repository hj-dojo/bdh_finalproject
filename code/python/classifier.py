import numpy
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVC, SVC
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier, AdaBoostClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import *


def predict(trainX, trainY, testX):
	# learner = GradientBoostingClassifier(n_estimators=100)
	#learner = RandomForestClassifier(n_estimators=100,criterion="gini",max_depth=8, random_state=207336481)
	learner = RandomForestClassifier(n_estimators=100, random_state=207336481)
	# learner = AdaBoostClassifier(n_estimators=100)

	# learner = DecisionTreeClassifier(min_samples_leaf=10)
	#learner = LogisticRegression()
	# learner = LinearSVC()
	# learner = SVC(kernel='poly')
	# learner = KNeighborsClassifier(n_neighbors=3)

	learner.fit(trainX, trainY)
	return learner.predict_proba(testX)


def getMetrics(truelabels, predictions):
	accuracy = accuracy_score(truelabels, predictions)
	auc = roc_auc_score(truelabels, predictions)
	precision = precision_score(truelabels, predictions)
	recall = recall_score(truelabels, predictions)
	f1 = f1_score(truelabels, predictions)

	return numpy.asarray([accuracy, auc, precision, recall, f1])


def getPrecisionRecallCurve(testY, predY, filename):
	precision, recall, _ = precision_recall_curve(testY, predY)
	plt.figure()
	plt.step(recall, precision, color='b', alpha=0.2, where='post')
	plt.fill_between(recall, precision, step='post', alpha=0.2, color='b')
	plt.xlabel('Recall')
	plt.ylabel('Precision')
	plt.xlim([0.0, 1.05])
	plt.ylim([0.0, 1.0])
	plt.title('Precision-Recall Curve')
	plt.savefig(filename)


def getROCCurve(testY, predY, auc, filename):
	fpr, tpr, _ = roc_curve(testY, predY)
	plt.figure()
	plt.plot(fpr, tpr, label='ROC Curve (area={0:.2f}'.format(auc))
	plt.plot([0,1],[0,1], 'k--')
	plt.xlim([0.,1.])
	plt.ylim([0.,1.05])
	plt.xlabel('False Positive Rate')
	plt.ylabel('True Positive Rate')
	plt.title('Receiver Operating Characteristic')
	plt.legend(loc='lower right')
	plt.savefig(filename)


def applyPredictionThresholding(probability_predictions, threshold=0.5):
	return numpy.asarray([ 1 if p[1] > threshold else 0 for p in probability_predictions ])




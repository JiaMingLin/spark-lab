import numpy as np
import pandas as pd
import itertools

from numpy import log, exp

class Base:
	def __init__(self):
		self.edges = []

	def sampling_rate(self, nrow, eps1):
		init_arr = np.linspace(0, 1, num=1000)
		beta_arr = init_arr[1:]
		all_binary = False

		def get_noise_scale(size, eps, x):
			N = size * x
			eps_alpha = self.amplify_epsilon_under_sampling(eps, x)
			sensitive_scale = self.gtest_sensitive_scale(N, all_binary)
			b = 2 * sensitive_scale / eps_alpha
			return b
		scale_beta_map = dict(zip(beta_arr, [get_noise_scale(nrow, eps1, b) for b in beta_arr]))
		return min(scale_beta_map, key = scale_beta_map.get)


	def mi_sensitive_scale(self, nrow, all_binary):
		if all_binary is True:
			sensitive_scale = (1 / nrow) * log(nrow) + ((nrow - 1) / nrow) * log(nrow / (nrow - 1))
		else:
			sensitive_scale = (2 / nrow) * log((nrow + 1) / 2) + ((nrow - 1) / nrow) * log((nrow + 1) / (nrow - 1))

		return sensitive_scale
		

	def gtest_sensitive_scale(self, nrow, all_binary):
		mi_sen = self.mi_sensitive_scale(nrow, all_binary)
		gtest_scale = 2 * mi_sen
		return gtest_scale

	def amplify_epsilon_under_sampling(self, eps, sample_rate):
		eps_alpha = log(exp(1) ** (eps) - 1 + sample_rate) - log(sample_rate)
		return eps_alpha

	def filter_association_edges(self, pair, measure, thresh):
		if measure >= thresh:
			self.edges.append(pair)

	@staticmethod
	def dependency(pair, broadcast_var):
		pandas_df = broadcast_var[pair]
		cols = pandas_df.columns[1:]
		mat = pandas_df.as_matrix(columns = cols)
		dl = mat.shape[0]; dk = mat.shape[1]
		expected_sum = self.get_expected_sum(mat)
		chi2 = np.sum((mat.T - expected_sum) ** 2 / expected_sum)
		CV = np.sqrt(chi2 / (nrow * (min(dk, dl) - 1)))
		return CV

	@staticmethod
	def pair_func_cnt(iterable, pairs):
		result = []
		pandas_df = pd.DataFrame(list(iterable))
		nrows = len(pandas_df)
		for p1, p2 in pairs:
			sub_val = zip(pandas_df[p1].tolist(), pandas_df[p2].tolist())
			sub_val = sorted(sub_val, key = lambda p: p)
			grped_cnt = [((p1, p2), [k[0], k[1], len(list(g))]) for k, g in itertools.groupby(sub_val, lambda p: p)]
			result += grped_cnt
		return iter(result)

	@staticmethod
	def g_test(iterable):
		pair_cnts = np.asarray(list(iterable))
		if len(pair_cnts) < 1: return iter([])

		pair_attrs = pair_cnts[:, 0][0]
		lv_cnts = pair_cnts[:, 1]

		agg = lambda g: sum(np.asarray(list(g))[:, 2])
		grp_sort_keyfunc = lambda e: [e[0], e[1]]

		_sorted = sorted(lv_cnts, key = lambda e: grp_sort_keyfunc(e))
		agg_lv_cnt = [[k[0], k[1], agg(g)] for k,g in itertools.groupby(_sorted, lambda e: grp_sort_keyfunc(e))]

		attr1_lvs = np.unique((np.asarray(agg_lv_cnt)[:,0]))
		attr2_lvs = np.unique((np.asarray(agg_lv_cnt)[:,1]))

		def get_fill_cnt(attr2_grp):
			from collections import OrderedDict
			grp_lv_cnt = np.asarray(attr2_grp)[:, [1,2]]
			full_cnt = OrderedDict(zip(list(attr2_lvs), np.zeros(len(attr2_lvs), dtype=int)))
			full_cnt.update(dict(grp_lv_cnt))
			return full_cnt.values()

		df = pd.DataFrame(agg_lv_cnt, columns = ['a1', 'a2', 'count'])
		contingency_tab = np.asarray([get_fill_cnt(df[df['a1'] == val]) for val in attr1_lvs])

		# expected sum
		exp_sum = Base.get_expected_sum(contingency_tab)

		chi2 = np.sum((contingency_tab.T - exp_sum) ** 2 / exp_sum)
		CV = np.sqrt(chi2 / (np.sum(contingency_tab) * (min(len(attr1_lvs), len(attr2_lvs)) - 1)))
		return iter([CV])

	@staticmethod
	def get_partition_size(iterable):
		agg_cnt = list(iterable)
		yield  len(agg_cnt)

	@staticmethod
	def get_expected_sum(xmat):
		rsums = np.sum(xmat, axis = 0).reshape(-1,1)
		csums = np.sum(xmat, axis = 1).reshape(1,-1)
		expected_sum = rsums * csums / float(np.sum(csums))
		return expected_sum

if __name__ == "__main__":
	eps_ls = [0.01,0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 ,1,10,100]
	nrow = 1000000
	base = Base()

	beta_ls = [base.sampling_rate(nrow, eps) for eps in eps_ls]
	print zip(eps_ls, beta_ls)
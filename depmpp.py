import multiprocessing

def init(np=None):
	'''Return a pool of work processes. If np is None, then the available number
	of processors in the machine is used'''

	return multiprocessing.Pool(np)

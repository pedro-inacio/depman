# the dependency manager module to be tested
import depman
import depmpp
import os

# used in functions
import time
import random

# Functions
def payload(nid):
	print 'Process ', os.getpid(), ': Node ' + nid + ': Zzzzzzzzz ...'
	time.sleep(1.0)
	print 'Process ', os.getpid(), ': Node ' + nid + ': AAAhhhhh! ...'

	return True

def simple_tree(n):
	'''Build a simple tree'''

	nodes = list()
	for i in range(n):
		nodes.append(depman.Node(pool,nodes[max(i-3,0):i]))

	return nodes
	
def random_tree(r,max_n):
	'''Build a random tree with r rows and a maximum of max_n nodes per row'''

	# refresh random generator
	random.seed()

	# populate tree row by row
	nodes = list()
	for ir in range(r):
		# number of nodes
		n = random.randrange(1,max_n)
		print 'n=',n
		if ir != 0:
			row = list()
			for in_ in range(n):
				print '	in=',in_
				# number of connections
				l = random.randrange(1,p+1)
				print '	l=',l
				# for each node select at most parents
				idx = set([random.randrange(p) for x in range(l)])
				print '	idx=',idx
				# append new node
				row.append(depman.Node(pool,[nodes[ir-1][x] for x in idx],payload=payload))
		else:
			# create root nodes
			row = [depman.Node(pool) for x in range(n)]

		# add new row
		nodes.append(row)

		# keep previous node number
		p = n

	return nodes

# Start the worker pool
pool = depmpp.init(1)

n = 20
print 'Building tree ...'
# nodes = random_tree(n,5)
nodes = simple_tree(n)
print 'Done building tree ...'
print 'Tree has ' + str(depman.gid) + ' nodes'

print 'Request build ...'
depman.update(nodes[n-6])
# print ','.join([str(x) for x in depman.traverse(nodes[6])])

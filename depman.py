#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Module to manage a tree of dependencies among nodes.

The aim of this module is to solve the same type of problems that GNU Make 
solves, i.e., to manage a chain of dependent actions in the right order and 
in parallel.
This module is intended to be abstract and application-agnostic so that it can
be used for any dependency problem in general.

In my specific case, I build this module because I need to manage a data 
processing chain: input files are processed by arbitrary programs yielding
output files. In turn, these are used inputs and so on. 
Another specific need is the fact that the processing chain I work on is not 
static; rather it is growing and changing over time. Some of the processing 
steps require large computational effort and therefore it is imperative to take 
advantage of avoiding unnecessary actions. A dependency chain reduces the 
problem to a set of nodes, where each node is a distinct processing step with 
inputs and outputs. The dependencis allow one to find tasks which can be run 
in parallel and by keeping track of changes in inputs allows one to decide 
which require recomputation. The system ensures correcteness and repoducibility,
the same fundamental requirements that GNU Make offers for the compilation of 
programs.

Initially I used ever-growing bash scripts scattered through multiple files 
which, not surprisingly, ended up bringing more problems than solutions. The 
idea of using GNU Make started to form, but, while possible, I found that the 
lack of a full programming language to construct the recipes (makefile) was a 
severe drawback. Then I found SCons, a python-based compilation tool which 
combines the power of GNU Make with the python language. SCons is a great tool 
which I have used to a large extent. The drawbacks of SCons are that it is 
specifically tailored to compilation tasks (which is great for people who want 
	to compile stuff!). As the number of files I managed kept on growing, it 
came to a point where the SCons became too slow to be useful. SCons is a complex
system offering a tremendous amount of functionality, but ultimately, I realized 
that I require the most basic functionality: a dependency manager. This is my 
attempt at it and my goal is to provide the basic funcionality and make it 
significantly faster than SCons.

The tree is made of nodes and they are linked in order to reflect their 
dependencies. The rules of the game are simple,
	If node A depends on B then, B is built after A
	If A changes, B must be updated to take the changes into account
	If A depends on B, B cannot depend on A or any of its children

Within a node, there are four ingredients required:
	1) the parents, other nodes upon which it depends
	2) a previously kept hash of the node to check for changes
	3) an action which is performed to update the node

Each node has parents, a action and a state. There is a single top level node, 
the root node. The user declares the set of nodes which are added to the tree
as desired. Once this is done, the user requests a (set of) node(s) to be built.
Each node has an internal thread. The large number of threads is commanded by a 
module semaphore. Each thread run the following algorithm,
	TODO

Each node is compared against an existing hash_table to check whether something
has changed.
The state of each node is one of the following,
	- 'undefined'	: initial value
	- 'new' 	 	: node not in the hashtable
	- 'changed'	 	: no changes in parents, but node has changed
	- 'old'			: node matches the hash table, but the value, number, or order of the
					  parents has changed
	- 'ok'			: no changes in parents, node matches hash table

There can be no circular dependencies, so no node can be a (grand-)child of 
itself. This is checked at runtime, traversing the tree upwards and gathering 
the full list of children at each node. [simpler!!]
'''

import dbm
import hashlib
import atexit

# NOTE: Temporary, only used for the sleep function
import time

## Metadata
__author__='Pedro Inácio'

## Module variables
# TODO: use efficient store for hash_table
# 		load at beggining, each node will update it in thread-safe way
# hash_table = dict()
hash_table = dbm.open('.depman','c')

# global node counter
gid = 0

# debug flag
DEBUG = True

## Module functions
def update(nodes):
	'''
	Traverse the tree requesting nodes to updated themselves until all
	declare to be updated
	'''

	# traverse
	trv = _traverse(nodes)

	# check all are up to date
	while not all([x.is_updated() for x in trv]):
		[x._trigger_update() for x in trv if not x.is_updated()]

def _traverse(nodes):
	'''
	Traverse the tree without using recursion. Return list of nodes sorted by 
	parents first
	'''

	# turn into list if not
	if not isinstance(nodes,list):
		nodes = [nodes]

	# make an internal copy of the list
	q = nodes[:]

	# trv returns a list of nodes parents first
	trv = []
	while q:
		x = q.pop(0)
		q.extend([y for y in x.parents if y not in q])

		if x in trv:
			trv.remove(x)
		# prepend parent at the top
		trv.insert(0, x)

	return trv

def payload(nid):
	'''
	Default payload of the base node.
	Can be replaced by any module level function which returns True for success,
	or False otherwise.
	'''

	print 'Node ' + nid + ': executing payload ...'
	return True

def _write_hashes(nid, hsh, parents):
	'''
	Write the hash of a node to the database along with its parents
	'''

	aux = str(hsh)
	for item in parents:
		aux += ',' + item[0] + ',' + item[1]

	hash_table[nid] = aux

def _read_hashes(nid):
	'''Read the hash and parents info from the hash table'''

	if nid not in hash_table:
		return None
	else:
		aux = hash_table[nid].split(',')
		hsh = aux[0]
		parents = list()
		for i in range(1,len(aux),2):
			parents.append(tuple([aux[i],aux[i+1]]))

		return hsh, parents

def _close_db():
	'''Close the db'''

	hash_table.close()

# tell python to close the db at exit
atexit.register(_close_db)

## Module classes
class Node(object):
	"""
	The Node is the most basic unit of the tree. 
	Subclasses are used to create specialized types of node, e.g., a File or an 
	Action.

	The Node requires a pool object to which a executing payload is delivered.
	parents is a list of parent Nodes. nid is the node id, one is automatically
	assigned if missing. Payload is a module-level function to be sent to the 
	execution queue before the node is declared as up-to-date.
	"""
	def __init__(self, pool, parents=[], nid=None, payload=payload):
		super(Node, self).__init__()

		# assign unique id
		if nid is None:
			global gid
			self.nid = str(gid)
			gid = gid + 1

		# add list of parents
		#  set to list if not none
		if not isinstance(parents,list):
			parents = list([parents])
		self.parents = parents

		# variables to keep track of events
		self._payload_delivered = False
		self._update_done = False

		# get the pool to submit jobs from the main program
		self._pool = pool

		# module level function to be executed in the processing pool
		# NOTE: This function is passed to the processing pool. Therefore, the 
		#		external processes must be aware of this function. In pratice,
		#		this means that the function must be declared before the 
		#		processing pool is created.
		# NOTE: This function must be pickled, therefore it must be a 
		#		module-level function.
		self._payload = payload

		# object storing the response object of the assynchronous call
		self._response = None

		## NOTE: at this point, the node's parents are set at instance creation
		#		 time. If the list of parents is not changed afterwards, cyclic
		#		 dependencies are automatically avoided.
		#		 If it becomes necessary to add new parents to a node, then 
		# 		 it also becomes necessary to check for cyclic dependencies at 
		# 		 the point where additional parents are included.
		# # this is used to check for closed loops in the tree
		# # could not think of a simpler method
		# self._all_parents = list()
		# [self._all_parents.extend(x._all_parents) for x in parents]
		# if self in self._all_parents:
		# 	raise ValueError('Cyclic dependency detected in node ' + self.nid)

	def __str__(self):

		return self.nid

	def __key(self):
		'''
		Return a string which can be used as a unique represntation of this 
		node.

		This string will be used to hash the node.
		'''

		return str(self.nid)

	def hash(self):
		'''
		Hashing function for node objects

		Takes the object key and runs it through the MD5 hash function.
		Notice that I do not overload the built-in hash() function because it 
		requires an integer to be returned. It is more useful to return a string 
		to store in the db.

		This is just a minimal example of a hash function. Subclasses of Node 
		should implement appropriate hash functions, e.g., a file node should 
		use the file contents to compute the hash.
		'''

		return hashlib.md5(str(self.__key())).hexdigest()

	def __eq__(x, y):
		'''Define node equality'''
		
		return x.__key() == y.__key()

	def is_updated(self):
		'''Return true if marked as up-to-date, otherwise trigger an update'''

		if self._update_done:
			return True
		else:
			return False

	def _trigger_update(self):
		'''
		After all parents are up to date, the node sends its payload to the pool
		'''

		# all parents must be uptodate
		if not all([x._update_done for x in self.parents]):
			[x._trigger_update() for x in self.parents if not x._update_done]

			if DEBUG:
				print 'Node ' + self.nid + ': update: wait parents'

		# check state, if it is ok then no need to recompute node
		elif self._check_state() == 'ok':

			if DEBUG:
				print 'Node ' + self.nid + ': update: no changes'

			# mark node as updated
			self._update()			
		
		# otherwise need to recompute the node
		# if payload was not delivered yet for processing, do it
		elif not self._payload_delivered:

			if DEBUG:
				print 'Node ' + self.nid + ': update: submit to pool'

			# deliver payload to the processing pool
			self._response = self._pool.apply_async(self._payload, (self.nid,), 
			 	callback=self._update_callback)
			# self.pool.apply(payload, (self.nid,))
			# self.payload_callback()

			# mark payload delivered
			self._payload_delivered = True

		# if payload has failed raise an exception
		elif self._response.ready() and not self._response.successful():

			raise RuntimeError('Failed to execute payload')

		# NOTE: when payload is done executing, the node callback function is 
		#		triggered which changes the status of the node. At this point 
		# 		this function will return immediately at the first if statement.

		# slow things down in debug mode
		if DEBUG:
			time.sleep(1.0)

		return

	def _update_callback(self, success):
		'''
		Callback function to set the update flag after the payload has been
		executed
		'''

		if DEBUG:
			print 'Node ' + self.nid + ': callback'

		if success:
			self._update()

		else:
			raise RuntimeError('Node ' + self.nid + ': error executing payload')

	def _update(self):
		'''Update my hash in the hash table and set the update flag'''

		# check that all parents are ready
		if not all([x._update_done for x in self.parents]):
			raise RuntimeError('All parents must be updated')

		# gather parents hashes
		list_parents_and_hashes = [(x.nid, x.hash()) for x in self.parents]

		# update hash_table
		_write_hashes(self.nid, self.hash(), list_parents_and_hashes)

		# mark node as updated
		self._update_done = True

	def _check_state(self):
		'''
		This runs a series of tests to determine the state of this node.
		The state is returned as a string at the end of this function.
		It should be executed after all the parents are up-to-date.
		An exception is raised if any parent is not marked as updated.
		'''

		# control variable
		return_flag = False

		# check that all parents are ready
		if not all([x._update_done for x in self.parents]):
			raise RuntimeError('All parents must be updated')

		# check that node exists
		if self.nid not in hash_table:
			if DEBUG:
				print 'Node ' + self.nid + ': _check_state: new'
			return 'new'

		# retrieve data from hash_table
		my_hash, list_parents_prev = _read_hashes(self.nid)

		# check that node did not change:
		if self.hash() != my_hash:
			if DEBUG:
				print 'Node ' + self.nid + ': _check_state: changed'
			return 'changed'

		# check that parents are the same:
		list_parents_now = [(x.nid, x.hash()) for x in self.parents]

		# check that no previous dependencies are missing
		for item in [x[0] for x in list_parents_prev]:
			if item not in [x[0] for x in list_parents_now]:
				# old dependencies gone
				if DEBUG:
					print 'Node ' + self.nid + ': _check_state: node', item, 'no longer a dependency'
				return_flag = True

		# check new dependencies
		for item in [x[0] for x in list_parents_now]:
			if item not in [x[0] for x in list_parents_prev]:
				# new dependencies
				if DEBUG:
					print 'Node ' + self.nid + ': _check_state: node', item, 'is a new dependency'
				return_flag = True

		if return_flag:
			return 'parents_number'

		# check the order of the dependencies
		for i in range(len(list_parents_now)):
			if list_parents_now[i][0] != list_parents_prev[i][0]:
				if DEBUG:
					print 'Node ' + self.nid + ': _check_state: order of dependencies changed'
				return 'parents_order'

		# check the hashes
		# NOTE: up to here we already checked the number and order of the 
		# 		elements in the list
		for i in range(len(list_parents_now)):
			if list_parents_now[i] != list_parents_prev[i]:
				if DEBUG:
					print 'Node ' + self.nid + ': _check_state: '. list_parents_now[i][0], ' has changed'
				return_flag = True
		
		if return_flag:		
			return 'parents_changed'

		# if none of the above apply, then all is good
		return 'ok'


#!/home/snikolenko/anaconda/bin/python
# -*- coding: utf8 -*

import os
from datetime import datetime,time,date,timedelta
from os import curdir,sep,path
import psycopg2,psycopg2.extras
import json

import tornado.ioloop
import tornado.web
import tornado.httpserver
from tornado.concurrent import Future
from tornado import gen
from tornado.ioloop import IOLoop
import tornpsql

import numpy as np

import time
import threading
import Queue
import decimal

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import cStringIO

from pyspark import SparkContext, SparkConf

from computing import *
import imaging
import imageentropy
import blockentropy

fulldataset_chunk_size = 1000

def my_print(s):
    print "[" + str(datetime.now()) + "] " + s

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return (datetime.min + obj).time().isoformat()
        else:
            return super(DateTimeEncoder, self).default(obj)

sql_counts = dict(
	formulas="SELECT count(*) FROM formulas",
	formulas_search="SELECT count(*) FROM formulas WHERE lower(name) like '%%%s%%' OR lower(sf) like '%%%s%%' OR id like '%s%%'",
	substancejobs="SELECT count(*) FROM jobs WHERE formula_id='%s'",
	jobs="SELECT count(*) FROM jobs",
	datasets="SELECT count(*) FROM datasets",
	fullimages="SELECT count(*) FROM job_result_stats WHERE job_id=%s"
)

sql_queries = dict(
	formulas="SELECT id,name,sf FROM formulas ",
	formulas_search="SELECT id,name,sf FROM formulas WHERE lower(name) like '%%%s%%' OR lower(sf) like '%%%s%%' OR id like '%s%%' ",
	substance="SELECT id,name,sf FROM formulas where id='%s'",
	substancejobs='''
		SELECT j.dataset_id,dataset,id,description,done,status,tasks_done,tasks_total,start,finish,id
		FROM jobs j
			LEFT JOIN datasets d on j.dataset_id=d.dataset_id
			LEFT JOIN job_types t on j.type=t.type
		WHERE formula_id='%s'
	''',
	jobs='''
		SELECT j.id as id,t.type,t.description,j.dataset_id,dataset,formula_id,f.name as formula_name,done,status,tasks_done,tasks_total,start,finish,j.id as id
		FROM jobs j LEFT JOIN datasets d on j.dataset_id=d.dataset_id
		LEFT JOIN formulas f on j.formula_id=f.id
		LEFT JOIN job_types t on t.type=j.type
	''',
	datasets='SELECT dataset_id,dataset,nrows,ncols,dataset_id FROM datasets',
	jobdescription='''
		SELECT j.dataset_id as dataset_id,dataset,description,done,status,tasks_done,tasks_total,start,finish
		FROM jobs j
			LEFT JOIN datasets d on j.dataset_id=d.dataset_id
			LEFT JOIN job_types t on j.type=t.type
		WHERE j.id=%s
	''',
	fullimages='''
		SELECT id,name,sf,stats->'entropy' as entropy,id
		FROM job_result_stats j LEFT JOIN formulas f ON f.id=cast(j.param as text)
		WHERE job_id=%s
	'''
)

sql_fields = dict(
	formulas=["id", "name", "sf"],
	substancejobs=["dataset_id", "dataset", "id", "description", "done", "status", "tasks_done", "tasks_total", "start", "finish", "id"],
	jobs=["id", "type", "description", "dataset_id", "dataset", "formula_id", "formula_name", "done", "status", "tasks_done", "tasks_total", "start", "finish", "id"],
	datasets=["dataset_id", "dataset", "nrows", "ncols", "dataset_id"],
	fullimages=["id", "name", "sf", "entropy", "id"]
)


def delayed(seconds):
	def f(x):
		time.sleep(seconds)
		return x
	return f

@gen.coroutine
def async_sleep(seconds):
    yield gen.Task(IOLoop.instance().add_timeout, time.time() + seconds)

def call_in_background(f, *args):
    result = Queue.Queue(1)
    t = threading.Thread(target=lambda: result.put(f(*args)))
    t.start()
    return result

def get_id_from_slug(slug):
	return slug if slug[-1] != '/' else slug[:-1]

dataset_name  = "Ctrl3s2"
# dataset_id  = 0
dataset_id  = 1
# dataset_name  = "test"
# dataset_fname = "/media/data/ims/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt"
# dataset_fname = "/home/snikolenko/soft/ims/data/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt"
dataset_fname = "/home/snikolenko/soft/ims/data/testdataset.txt"

def run_extractmzs(sc, fname, data, nrows, ncols):
	ff = sc.textFile(fname)
	spectra = ff.map(txt_to_spectrum)
	qres = spectra.map(lambda sp : get_many_groups_total_dict(data, sp)).reduce(join_dicts)
	return qres

def dicts_to_dict(dictresults):
	res_dict = dictresults[0]
	for res in dictresults[1:]:
		res_dict.update({ k : v + res_dict.get(k, 0.0) for k,v in res.iteritems() })
	return res_dict

def run_fulldataset(sc, fname, data, nrows, ncols):
	ff = sc.textFile(fname)
	spectra = ff.map(txt_to_spectrum)
	qres = spectra.map(lambda sp : get_many_groups2d_total_dict(data, sp)).reduce(reduce_manygroups2d_dict)
	entropies = [ blockentropy.get_block_entropy_dict(x, nrows, ncols) for x in qres ]
	return (qres, entropies)


class RunSparkHandler(tornado.web.RequestHandler):
	@property
	def db(self):
		return self.application.db

	def result_callback(response):
		my_print("Got response! %s" % response)

	def strings_to_dict(self, stringresults):
		res_dict = { int(x.split(':')[0]) : float(x.split(':')[1]) for x in stringresults[0].split(' ') }
		for res_string in stringresults[1:]:
			res_dict.update({ int(x.split(':')[0]) : float(x.split(':')[1]) + res_dict.get(int(x.split(':')[0]), 0.0) for x in res_string.split(' ') })
		return res_dict

	def process_res_extractmzs(self, result):
		res_dict = result.get()
		my_print("Got result of job %d with %d nonzero spectra" % (self.job_id, len(res_dict)))
		if (len(res_dict) > 0):
			self.db.query("INSERT INTO job_result_data VALUES %s" %
				",".join(['(%d, %d, %d, %.6f)' % (self.job_id, -1, k, v) for k,v in res_dict.iteritems()])
			)

	def process_res_fulldataset(self, result, offset=0):
		res_dicts, entropies = result.get()
		total_nonzero = sum([len(x) for x in res_dicts])
		my_print("Got result of full dataset job %d with %d nonzero spectra" % (self.job_id, total_nonzero))
		if (total_nonzero > 0):
			self.db.query("INSERT INTO job_result_data VALUES %s" %
				",".join(['(%d, %d, %d, %.6f)' % (self.job_id, int(self.formulas[i+offset]["id"]), k, v) for i in xrange(len(res_dicts)) for k,v in res_dicts[i].iteritems()])
			)
			self.db.query("INSERT INTO job_result_stats VALUES %s" %
				",".join(['(%d, %d, \'{"entropy" : %.6f}\')' % (
					self.job_id,
					int(self.formulas[i+offset]["id"]),
					entropies[i]
				) for i in xrange(len(res_dicts)) if entropies[i] > 0 ])
			)

	@gen.coroutine
	def post(self, query_id):
		my_print("called /run/" + query_id)

		self.dataset_id = int(self.get_argument("dataset_id"))
		dataset_params = self.db.query("SELECT filename,nrows,ncols FROM datasets WHERE dataset_id=%d" % self.dataset_id)[0]
		self.nrows = dataset_params["nrows"]
		self.ncols = dataset_params["ncols"]
		self.fname = dataset_params["filename"]
		self.job_id = -1
		## we want to extract m/z values
		if query_id == "extractmzs":
			self.formula_id = self.get_argument("formula_id")
			self.job_type = 0
			tol = 0.01
			peaks = self.db.query("SELECT peaks FROM mz_peaks WHERE formula_id='%s'" % self.formula_id)[0]["peaks"]
			# data = [ [float(x)-tol, float(x)+tol] for x in self.get_argument("data").strip().split(',')]
			data = [ [float(x)-tol, float(x)+tol] for x in peaks]
			my_print("Running m/z extraction for formula id %s" % self.formula_id)

			cur_jobs = set(self.application.status.getActiveJobsIds())
			my_print("Current jobs: %s" % cur_jobs)
			result = call_in_background(run_extractmzs, *(self.application.sc, self.fname, data, self.nrows, self.ncols))
			self.spark_job_id = -1
			while self.spark_job_id == -1:
				yield async_sleep(1)
				my_print("Current jobs: %s" % set(self.application.status.getActiveJobsIds()))
				if len(set(self.application.status.getActiveJobsIds()) - cur_jobs) > 0:
					self.spark_job_id = list(set(self.application.status.getActiveJobsIds()) - cur_jobs)[0]
			## if this job hasn't started yet, add it
			if self.job_id == -1:
				self.job_id = self.application.add_job(self.spark_job_id, self.formula_id, self.dataset_id, self.job_type, datetime.now())
			else:
				self.application.jobs[self.job_id]["spark_id"] = self.spark_job_id
			while result.empty():
				yield async_sleep(1)
			self.process_res_extractmzs(result)

		elif query_id == "fulldataset":
			my_print("Running dataset-wise m/z image extraction for dataset id %s" % self.dataset_id)
			self.formula_id = -1
			self.job_type = 1			
			prefix = "\t[fullrun %s] " % self.dataset_id
			my_print(prefix + "collecting m/z queries for the run")
			tol = 0.01
			self.formulas = self.db.query("SELECT formula_id as id,peaks FROM mz_peaks")
			mzpeaks = [ x["peaks"] for x in self.formulas]
			data = [ [ [float(x)-tol, float(x)+tol] for x in peaks ] for peaks in mzpeaks ]
			my_print(prefix + "looking for %d peaks" % sum([len(x) for x in data]))
			self.num_chunks = len(data) / fulldataset_chunk_size
			self.job_id = self.application.add_job(-1, self.formula_id, self.dataset_id, self.job_type, datetime.now(), chunks=self.num_chunks)
			for i in xrange(self.num_chunks):
				my_print("Processing chunk %d..." % i)
				cur_jobs = set(self.application.status.getActiveJobsIds())
				my_print("Current jobs: %s" % cur_jobs)
				result = call_in_background(run_fulldataset, *(self.application.sc, self.fname, data[fulldataset_chunk_size*i:fulldataset_chunk_size*(i+1)], self.nrows, self.ncols))
				self.spark_job_id = -1
				while self.spark_job_id == -1:
					yield async_sleep(1)
					my_print("Current jobs: %s" % set(self.application.status.getActiveJobsIds()))
					if len(set(self.application.status.getActiveJobsIds()) - cur_jobs) > 0:
						self.spark_job_id = list(set(self.application.status.getActiveJobsIds()) - cur_jobs)[0]
				## if this job hasn't started yet, add it
				if self.job_id == -1:
					self.job_id = self.application.add_job(self.spark_job_id, self.formula_id, self.dataset_id, self.job_type, datetime.now())
				else:
					self.application.jobs[self.job_id]["spark_id"] = self.spark_job_id
				while result.empty():
					yield async_sleep(1)
				self.process_res_fulldataset(result, offset=fulldataset_chunk_size*i)
		else:
			my_print("[ERROR] Incorrect run query %s!" % query_id)
			return

class MZImageHandler(tornado.web.RequestHandler):
	@property
	def db(self):
		return self.application.db

	@gen.coroutine
	def get(self, job_string):
		my_print(job_string)
		job_id = int(get_id_from_slug(job_string))
		my_print("Creating m/z image for job %d..." % job_id)
		params = self.db.query("SELECT nrows,ncols FROM jobs j JOIN datasets d on j.dataset_id=d.dataset_id WHERE j.id=%d" % (int(job_id)))[0]
		(dRows, dColumns) = ( int(params["nrows"]), int(params["ncols"]) )
		data = self.db.query("SELECT spectrum as s,value as v FROM job_result_data WHERE job_id=%d" % (int(job_id)))
		sio = imaging.write_image( imaging.make_image_arrays(dRows, dColumns, [int(row["s"]) for row in data], [float(row["v"]) for row in data]) )
		self.set_header("Content-Type", "image/png")
		self.write(sio.getvalue())


class MZImageParamHandler(tornado.web.RequestHandler):
	@property
	def db(self):
		return self.application.db

	@gen.coroutine
	def get(self, job_string, param_string):
		my_print(job_string)
		job_id, param_id = int(get_id_from_slug(job_string)), int(get_id_from_slug(param_string))
		my_print("Creating m/z image for job %d..." % job_id)
		params = self.db.query("SELECT nrows,ncols FROM jobs j JOIN datasets d on j.dataset_id=d.dataset_id WHERE j.id=%d" % (int(job_id)))[0]
		(dRows, dColumns) = ( int(params["nrows"]), int(params["ncols"]) )
		data = self.db.query("SELECT spectrum as s,value as v FROM job_result_data WHERE job_id=%d AND param=%d" % (int(job_id), int(param_id)))
		sio = imaging.write_image( imaging.make_image_arrays(dRows, dColumns, [int(row["s"]) for row in data], [float(row["v"]) for row in data]) )
		self.set_header("Content-Type", "image/png")
		self.write(sio.getvalue())


class AjaxHandler(tornado.web.RequestHandler):
	@property
	def db(self):
		return self.application.db

	def make_datatable_dict(self, draw, count, res):
		return {
			"draw":             draw,
			"recordsTotal":     count,
			"recordsFiltered":  count,
			"data":             res    
		}

	@gen.coroutine
	def get(self, query_id, slug):
		my_print("ajax %s starting..." % query_id)
		my_print("%s" % query_id)
		my_print("%s" % slug)
		draw = self.get_argument('draw', 0)
		input_id = ""
		if len(slug) > 0:
			input_id = get_id_from_slug(slug)
		
		if query_id in ['formulas', 'substancejobs', 'jobs', 'datasets']:
			orderby = sql_fields[query_id][ int(self.get_argument('order[0][column]', 0)) ]
			orderdir = self.get_argument('order[0][dir]', 0)
			limit = self.get_argument('length', 0)
			offset = self.get_argument('start', 0)
			searchval = self.get_argument('search[value]', "")
			my_print("search for : %s" % searchval)

			## queries
			q_count = sql_counts[query_id] if searchval == "" else (sql_counts[query_id + '_search'] % (searchval, searchval, searchval))
			q_res = sql_queries[query_id] if searchval == "" else (sql_queries[query_id + '_search'] % (searchval, searchval, searchval))
			if query_id in ['substancejobs', 'fullimages']:
				q_count = q_count % input_id
				q_res = q_res % input_id
			my_print(q_count)
			my_print(q_res + " ORDER BY %s %s LIMIT %s OFFSET %s" % (orderby, orderdir, limit, offset))
			count = int(self.db.query(q_count)[0]['count'])
			res = self.db.query(q_res + " ORDER BY %s %s LIMIT %s OFFSET %s" % (orderby, orderdir, limit, offset))
			res_dict = self.make_datatable_dict(draw, count, [[ row[x] for x in sql_fields[query_id] ] for row in res])
		else:
			my_print(sql_queries[query_id] % input_id)
			res_list = self.db.query(sql_queries[query_id] % input_id)
			if query_id not in ['fullimages']:
				res_dict = res_list[0]
			else:
				res_dict = {"data" : [ [x[field] for field in sql_fields[query_id]] for x in res_list]}
			## add isotopes for the substance query
			if query_id == "substance":
				# my_print("mz lists: %s" % get_lists_of_mzs(res_dict["sf"]))
				res_dict.update(get_lists_of_mzs(res_dict["sf"]))
			res_dict.update({"draw" : draw})

		my_print("ajax %s processed, returning..." % query_id)
		self.write(json.dumps(res_dict, cls = DateTimeEncoder))

class IndexHandler(tornado.web.RequestHandler):
	@gen.coroutine
	def get(self):
		self.render("index.html")

html_pages = {
}

class SimpleHtmlHandlerWithId(tornado.web.RequestHandler):
	@gen.coroutine
	def get(self, id):
		my_print("Request: %s, Id: %s" % (self.request.uri, id))
		self.render( html_pages.get( self.request.uri.split('/')[1], self.request.uri.split('/')[1] + ".html") )

class SimpleHtmlHandler(tornado.web.RequestHandler):
	@gen.coroutine
	def get(self):
		my_print("Request: %s" % self.request.uri)
		self.render( html_pages.get( self.request.uri.split('/')[1], self.request.uri.split('/')[1] + ".html") )

class Application(tornado.web.Application):
	def __init__(self):
		handlers = [
			(r"^/ajax/([a-z]*)/(.*)", AjaxHandler),
			(r"^/substance/(.*)", SimpleHtmlHandlerWithId),
			(r"^/run/(.*)", RunSparkHandler),
			(r"^/mzimage/([^/]*)\.png", MZImageHandler),
			(r"^/mzimage/([^/]*)/([^/]*)\.png", MZImageParamHandler),
			(r"^/jobs/", SimpleHtmlHandler),
			(r"^/datasets/", SimpleHtmlHandler),
			(r"^/fullresults/(.*)", SimpleHtmlHandlerWithId),
			(r"/", IndexHandler)
		]
		settings = dict(
			static_path=path.join(os.path.dirname(__file__), "static"),
			debug=True
		)
		config_db = dict(
		    host="/var/run/postgresql/",
		    db="ims",
		    user="snikolenko",
		    password=""
		)
		tornado.web.Application.__init__(self, handlers, **settings)
		# Have one global connection to the blog DB across all handlers
		self.db = tornpsql.Connection(config_db['host'], config_db['db'], config_db['user'], config_db['password'], 5432)
		self.conf = SparkConf().setMaster("local[2]").setAppName("IMS Webserver v0.2").set("spark.ui.showConsoleProgress", "false")
		self.sc = SparkContext(conf=self.conf)
		self.status = self.sc.statusTracker()
		self.max_jobid = self.db.get("SELECT max(id) as maxid FROM jobs").maxid
		self.max_jobid = int(self.max_jobid) if self.max_jobid != None else 0
		self.jobs = {}

	def get_next_job_id(self):
		self.max_jobid += 1
		return self.max_jobid

	def add_job(self, spark_id, formula_id, data_id, job_type, started, chunks=1):
		job_id = self.get_next_job_id()
		self.jobs[job_id] = {
			"type" : job_type,
			"spark_id" : spark_id,
			"formula_id" : formula_id,
			"started" : started,
			"finished" : started,
			"chunks" : chunks,
			"chunk_size" : 0,
			"chunks_done" : 0
		}
		self.db.query('''
			INSERT INTO jobs VALUES (%d, %d, '%s', %d, false, 'RUNNING', %d, %d, '%s', '%s')
		''' % (job_id, job_type, formula_id, data_id, 0, 0, str(started), str(started)) )
		return job_id


	def update_all_jobs_callback(self):
		try:
			my_print("updating spark jobs status...")
			for job_id in self.jobs:
				self.update_job_status(job_id)
		finally:
			tornado.ioloop.IOLoop.instance().add_timeout(timedelta(seconds=5), self.update_all_jobs_callback)

	def update_job_status(self, job_id):
		v = self.jobs[job_id]
		jobinfo = self.status.getJobInfo(v["spark_id"])
		done_string = 'false' if jobinfo.status == 'RUNNING' else 'true'
		total_total = v["chunk_size"] * v["chunks"]
		if v["finished"] == v["started"] and done_string == "true":
			v["chunks_done"] += 1
			if v["chunks_done"] == v["chunks"]:
				v["finished"] = datetime.now()
			total_done = v["chunk_size"] * v["chunks_done"]
		else:
			(nTasks, nActive, nComplete) = (0, 0, 0)
			for sid in jobinfo.stageIds:
				stageinfo = self.status.getStageInfo(sid)
				if stageinfo:
					nTasks += stageinfo.numTasks
					nActive += stageinfo.numActiveTasks
					nComplete += stageinfo.numCompletedTasks
				if v["chunks"] > 0 and v["chunk_size"] == 0:
					v["chunk_size"] = nTasks
			total_done = v["chunk_size"] * v["chunks_done"] + nComplete
		total_done = min(total_done, total_total)
		my_print("Setting job totals: %d %d %d %d %d" % (v["chunk_size"], v["chunks"], v["chunks_done"], total_total, total_done))
		self.db.query('''
			UPDATE jobs SET tasks_done=%d, tasks_total=%d, status='%s', done=%s, finish='%s'
			WHERE id=%d
		''' % (total_done, total_total, jobinfo.status, done_string, str(self.jobs[job_id]["finished"]), job_id))

def main():
	try:
		port = 2347
		torn_app = Application()
		http_server = tornado.httpserver.HTTPServer(torn_app)
		http_server.listen(port)
		my_print("Starting server, listening to port %d..." % port)
		## set periodic updates
		tornado.ioloop.IOLoop.instance().add_timeout(timedelta(seconds=5), torn_app.update_all_jobs_callback)
		## start loop
		tornado.ioloop.IOLoop.instance().start()
	except KeyboardInterrupt:
		my_print( '^C received, shutting down server' )
		torn_app.sc.stop()
		http_server.socket.close()


if __name__ == "__main__":
    main()



# config_db = dict(
#                     host="/var/run/postgresql/",
#                     db="ims",
#                     user="snikolenko",
#                     password=""
#                 )
# conn = psycopg2.connect("host='%s' dbname='%s' user='%s' password='%s'" % (
#     config_db["host"], config_db["db"], config_db["user"], config_db["password"]
#     ))
# cur = conn.cursor()
# cur.execute("SELECT id,sf FROM formulas")
# rows = cur.fetchall()

# mzpeaks = {}
# for x in rows:
# 	mzpeaks[x[0]] = get_lists_of_mzs(x[1])["grad_mzs"]

# with open("mzpeaks.csv", "w") as outfile:
# 	outfile.write("\n".join(["%s;{%s}" % (k, ",".join(["%.4f" % x for x in mzpeaks[k]]) ) for k in mzpeaks if len(mzpeaks[k]) > 0 ]))



# cur.execute("INSERT INTO job_result_data VALUES %s" %
# 				",".join(['(%d, %d, %d, %.6f)' % (self.job_id, -1, k, v) for k,v in res_dict.iteritems()])
# 			)


